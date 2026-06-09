/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/search', 'N/record', 'N/log'], function (search, record, log) {

    var SAVED_SEARCH_ID = 'customsearch_rh_3pl_order_status_integra';

    var BODY_STATUS_FIELD = 'custbody_3pl_export_status';
    var LINE_STATUS_FIELD = 'custcol_3pl_export_status';

    // New decimal field on Sales Order body
    // Replace with actual field ID if different
    var BODY_3PL_COUNT_FIELD = 'custbody_3pl_order_count';

    var STATUS_READY = '1';
    var STATUS_SENT = '2';
    var STATUS_ERROR = '3';
    var STATUS_NOT_RELEASED = '4';
    var STATUS_PARTIAL_READY = '5';
    var STATUS_PARTIAL_SENT = '6';
    var STATUS_FULFILLED = '7';
    var STATUS_PARTIAL_FULFILLED = '8';
    var STATUS_MANUAL_HOLD = '9';

    function getInputData() {
        log.audit('getInputData', 'Loading search: ' + SAVED_SEARCH_ID);

        return search.load({
            id: SAVED_SEARCH_ID
        });
    }

    function map(context) {
        try {
            var row = JSON.parse(context.value);
            var soId = '';

            if (row.values && row.values.internalid) {
                soId = row.values.internalid.value || row.values.internalid;
            } else if (row.values && row.values['GROUP(internalid)']) {
                soId = row.values['GROUP(internalid)'].value || row.values['GROUP(internalid)'];
            } else if (row.id) {
                soId = row.id;
            }

            if (!soId) {
                log.error('SO ID Missing', context.value);
                return;
            }

            context.write({
                key: soId,
                value: soId
            });

        } catch (e) {
            log.error('MAP ERROR', e);
        }
    }

    function reduce(context) {
        try {
            var soId = context.key;

            log.audit('REDUCE START', {
                soId: soId
            });

            var soRec = record.load({
                type: record.Type.SALES_ORDER,
                id: soId,
                isDynamic: false
            });

            var bodyStatus = String(soRec.getValue({
                fieldId: BODY_STATUS_FIELD
            }) || '');

            /*
             * Body protected statuses.
             * Do not process full Sent, Error, Fulfilled, Manual Hold.
             * Partial Sent is NOT protected because later remaining line can become Ready.
             */
            if (isProtectedBodyStatus(bodyStatus)) {
                log.audit('BODY STATUS SKIPPED', {
                    soId: soId,
                    bodyStatus: bodyStatus
                });
                return;
            }

            var lineCount = soRec.getLineCount({
                sublistId: 'item'
            });

            var changed = false;

            var hasProcessLine = false;
            var hasReadyLine = false;
            var hasSentLine = false;
            var hasBlankLine = false;

            for (var i = 0; i < lineCount; i++) {

                var quantity = toNumber(soRec.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'quantity',
                    line: i
                }));

                var quantityCommitted = toNumber(soRec.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'quantitycommitted',
                    line: i
                }));

                var currentLineStatus = String(soRec.getSublistValue({
                    sublistId: 'item',
                    fieldId: LINE_STATUS_FIELD,
                    line: i
                }) || '');

                /*
                 * Sent/Error/Fulfilled/Manual Hold lines should not be changed.
                 * But Sent line is still used for header calculation.
                 */
                if (currentLineStatus === STATUS_SENT) {
                    hasProcessLine = true;
                    hasSentLine = true;

                    log.debug('LINE ALREADY SENT - NOT TOUCHED', {
                        soId: soId,
                        line: i,
                        currentLineStatus: currentLineStatus
                    });

                    continue;
                }

                if (isHardProtectedLineStatus(currentLineStatus)) {
                    log.debug('LINE PROTECTED - NOT TOUCHED', {
                        soId: soId,
                        line: i,
                        currentLineStatus: currentLineStatus
                    });

                    continue;
                }

                hasProcessLine = true;

                var targetLineStatus = '';

                /*
                 * New line logic:
                 * Ordered Qty = Committed Qty  => Ready To Send
                 * Ordered Qty != Committed Qty => Blank
                 */
                if (quantity > 0 && isSameQty(quantity, quantityCommitted)) {
                    targetLineStatus = STATUS_READY;
                } else {
                    targetLineStatus = '';
                }

                if (targetLineStatus === STATUS_READY) {
                    hasReadyLine = true;
                } else {
                    hasBlankLine = true;
                }

                if (currentLineStatus !== targetLineStatus) {
                    soRec.setSublistValue({
                        sublistId: 'item',
                        fieldId: LINE_STATUS_FIELD,
                        line: i,
                        value: targetLineStatus
                    });

                    changed = true;

                    log.debug('LINE STATUS UPDATED', {
                        soId: soId,
                        line: i,
                        quantity: quantity,
                        quantityCommitted: quantityCommitted,
                        oldStatus: currentLineStatus,
                        newStatus: targetLineStatus
                    });
                }
            }

            var newBodyStatus = bodyStatus;
            var shouldUpdateBodyStatus = false;

            /*
             * Header logic:
             *
             * Ready + Ready  => Ready To Send
             * Ready + Blank  => Partly Ready
             * Sent + Ready   => Ready To Send
             * Sent + Blank   => Do not touch header
             */
            if (hasProcessLine) {

                if (hasReadyLine && !hasBlankLine) {
                    // Ready + Ready
                    // Sent + Ready
                    newBodyStatus = STATUS_READY;
                    shouldUpdateBodyStatus = true;

                } else if (hasReadyLine && hasBlankLine && !hasSentLine) {
                    // Ready + Blank
                    newBodyStatus = STATUS_PARTIAL_READY;
                    shouldUpdateBodyStatus = true;

                } else if (hasReadyLine && hasBlankLine && hasSentLine) {
                    /*
                     * Sent + Ready + Blank
                     * This means some lines sent, some ready, some still not ready.
                     * Based on your simple flow, keep it Partly Ready so the ready line can go.
                     */
                    newBodyStatus = STATUS_PARTIAL_READY;
                    shouldUpdateBodyStatus = true;

                } else if (hasSentLine && hasBlankLine && !hasReadyLine) {
                    // Sent + Blank = do not touch header
                    shouldUpdateBodyStatus = false;

                    log.debug('HEADER NOT TOUCHED - SENT + BLANK', {
                        soId: soId,
                        currentBodyStatus: bodyStatus
                    });

                } else {
                    // Blank only = blank header
                    newBodyStatus = '';
                    shouldUpdateBodyStatus = true;
                }
            }

            var bodyStatusChanged = false;

            if (shouldUpdateBodyStatus && bodyStatus !== newBodyStatus) {
                soRec.setValue({
                    fieldId: BODY_STATUS_FIELD,
                    value: newBodyStatus
                });

                changed = true;
                bodyStatusChanged = true;

                log.debug('BODY STATUS UPDATED', {
                    soId: soId,
                    oldStatus: bodyStatus,
                    newStatus: newBodyStatus
                });
            }

            /*
             * 3PL Count Logic:
             * Increase count only when header status changes to:
             * - Ready To Send
             * - Partly Ready
             */
            if (
                bodyStatusChanged &&
                (
                    newBodyStatus === STATUS_READY ||
                    newBodyStatus === STATUS_PARTIAL_READY
                )
            ) {
                var oldCount = toNumber(soRec.getValue({
                    fieldId: BODY_3PL_COUNT_FIELD
                }));

                var newCount = oldCount + 1;

                soRec.setValue({
                    fieldId: BODY_3PL_COUNT_FIELD,
                    value: newCount
                });

                changed = true;

                log.debug('3PL COUNT UPDATED', {
                    soId: soId,
                    oldCount: oldCount,
                    newCount: newCount,
                    oldBodyStatus: bodyStatus,
                    newBodyStatus: newBodyStatus
                });
            }

            if (changed) {
                var saveId = soRec.save({
                    enableSourcing: false,
                    ignoreMandatoryFields: true
                });

                log.audit('SALES ORDER SAVED', {
                    soId: soId,
                    saveId: saveId,
                    oldBodyStatus: bodyStatus,
                    newBodyStatus: newBodyStatus
                });
            } else {
                log.debug('NO CHANGE', {
                    soId: soId,
                    bodyStatus: bodyStatus
                });
            }

        } catch (e) {
            log.error('REDUCE ERROR', {
                soId: context.key,
                message: e.message,
                stack: e.stack
            });
        }
    }

    function isProtectedBodyStatus(status) {
        status = String(status || '');

        return (
            status === STATUS_SENT ||
            status === STATUS_ERROR ||
            status === STATUS_FULFILLED ||
            status === STATUS_MANUAL_HOLD
        );
    }

    function isHardProtectedLineStatus(status) {
        status = String(status || '');

        return (
            status === STATUS_ERROR ||
            status === STATUS_FULFILLED ||
            status === STATUS_MANUAL_HOLD
        );
    }

    function toNumber(value) {
        var num = Number(value || 0);
        return isNaN(num) ? 0 : num;
    }

    function isSameQty(qty1, qty2) {
        qty1 = toNumber(qty1);
        qty2 = toNumber(qty2);

        return Math.abs(qty1 - qty2) < 0.000001;
    }

    function summarize(summary) {
        log.audit('SUMMARY', {
            usage: summary.usage,
            concurrency: summary.concurrency,
            yields: summary.yields
        });

        summary.mapSummary.errors.iterator().each(function (key, error) {
            log.error('MAP SUMMARY ERROR', {
                key: key,
                error: error
            });
            return true;
        });

        summary.reduceSummary.errors.iterator().each(function (key, error) {
            log.error('REDUCE SUMMARY ERROR', {
                key: key,
                error: error
            });
            return true;
        });
    }

    return {
        getInputData: getInputData,
        map: map,
        reduce: reduce,
        summarize: summarize
    };
});
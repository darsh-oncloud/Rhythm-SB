/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/search', 'N/record', 'N/log'], function (search, record, log) {

    var SAVED_SEARCH_ID = 'customsearch_rh_3pl_order_status_integra';

    var BODY_STATUS_FIELD = 'custbody_3pl_export_status';
    var LINE_STATUS_FIELD = 'custcol_3pl_export_status';

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

            if (isProtectedStatus(bodyStatus)) {
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
            var hasEligibleLine = false;
            var hasReadyOrPartial = false;
            var allEligibleLinesReady = true;

            for (var i = 0; i < lineCount; i++) {

                var quantity = Number(soRec.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'quantity',
                    line: i
                }) || 0);

                var quantityCommitted = Number(soRec.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'quantitycommitted',
                    line: i
                }) || 0);

                var quantityFulfilled = Number(soRec.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'quantityfulfilled',
                    line: i
                }) || 0);

                var currentLineStatus = String(soRec.getSublistValue({
                    sublistId: 'item',
                    fieldId: LINE_STATUS_FIELD,
                    line: i
                }) || '');

                if (isProtectedStatus(currentLineStatus)) {
                    continue;
                }

                hasEligibleLine = true;

                var targetLineStatus = '';

                if (quantityCommitted > 0) {
                    if ((quantityCommitted + quantityFulfilled) >= quantity) {
                        targetLineStatus = STATUS_READY;
                    } else {
                        targetLineStatus = STATUS_PARTIAL_READY;
                    }
                } else {
                    targetLineStatus = '';
                }

                if (targetLineStatus === STATUS_READY || targetLineStatus === STATUS_PARTIAL_READY) {
                    hasReadyOrPartial = true;
                }

                if (targetLineStatus !== STATUS_READY) {
                    allEligibleLinesReady = false;
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
                        quantityFulfilled: quantityFulfilled,
                        oldStatus: currentLineStatus,
                        newStatus: targetLineStatus
                    });
                }
            }

            var newBodyStatus = '';

            if (hasEligibleLine && allEligibleLinesReady) {
                newBodyStatus = STATUS_READY;
            } else if (hasEligibleLine && hasReadyOrPartial) {
                newBodyStatus = STATUS_PARTIAL_READY;
            } else {
                newBodyStatus = '';
            }

            if (bodyStatus !== newBodyStatus) {
                soRec.setValue({
                    fieldId: BODY_STATUS_FIELD,
                    value: newBodyStatus
                });

                changed = true;

                log.debug('BODY STATUS UPDATED', {
                    soId: soId,
                    oldStatus: bodyStatus,
                    newStatus: newBodyStatus
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
                    newBodyStatus: newBodyStatus
                });
            } else {
                log.debug('NO CHANGE', {
                    soId: soId
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

    function isProtectedStatus(status) {
        status = String(status || '');

        return (
            status === STATUS_SENT ||
            status === STATUS_ERROR ||
            status === STATUS_FULFILLED ||
            status === STATUS_MANUAL_HOLD
        );
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

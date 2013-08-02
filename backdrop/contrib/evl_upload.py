from datetime import datetime
import itertools


def ceg_volumes(sheets):
    """Electronic Vehicle Licensing (EVL) Customer Enquiries Group (CEG)

    Call center volume data

    http://goo.gl/52VcMe
    """
    RELICENSING_WEB_INDEX = 5
    RELICENSING_IVR_INDEX = 6
    RELICENSING_AGENT_INDEX = 9
    SORN_WEB_INDEX = 11
    SORN_IVR_INDEX = 12
    SORN_AGENT_INDEX = 13
    AGENT_AUTOMATED_DUPES_INDEX = 15
    CALLS_ANSWERED_BY_ADVISOR_INDEX = 17

    ceg_keys = [
            "_timestamp", "timeSpan", "relicensing_web", "relicensing_ivr",
            "relicensing_agent", "sorn_web", "sorn_ivr", "sorn_agent",
            "agent_automated_dupes", "calls_answered_by_advisor"
        ]

    def ceg_rows(sheets):
        rows = list(list(sheets)[0])
        for column in itertools.count(3):
            date = ceg_date(rows, column)
            if not isinstance(date, datetime):
                return
            yield [
                date, "month",
                rows[RELICENSING_WEB_INDEX][column],
                rows[RELICENSING_IVR_INDEX][column],
                rows[RELICENSING_AGENT_INDEX][column],
                rows[SORN_WEB_INDEX][column],
                rows[SORN_IVR_INDEX][column],
                rows[SORN_AGENT_INDEX][column],
                rows[AGENT_AUTOMATED_DUPES_INDEX][column],
                rows[CALLS_ANSWERED_BY_ADVISOR_INDEX][column],
            ]

    def ceg_date(rows, column):
        try:
            return rows[3][column]
        except IndexError:
            return None

    yield ceg_keys

    for row in ceg_rows(sheets):
        yield row


def service_volumetrics(sheets):
    rows = list(list(sheets)[0])
    yield ["_timestamp", "timeSpan", "successful_tax_disc", "successful_sorn"]

    timestamp = rows[2][1]
    taxDiskApplications = rows[24][2]
    sornApplications = rows[25][2]

    yield [timestamp, "day", taxDiskApplications, sornApplications]

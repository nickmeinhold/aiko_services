# Usage
# ~~~~~
# pytest [-s] unit/test_dashboard_filter.py
# pytest [-s] unit/test_dashboard_filter.py::test_a_process_without_service_id_1
#
# Regression tests for the Service list filter in "dashboard.py".
#
# The Dashboard groups a Process's Services under the Service that holds
# "service_id" 1, and the "pipeline_element" filter hides the other Services of
# a Process whose Service 1 is a Pipeline.
#
# The bug these cover: the parent was carried through one loop across every
# Service in the list, and the list holds the Services of all Processes. The
# parent changed only when a Service with "service_id" 1 was found, so a Process
# that has no Service 1 used the previous Process's parent, and the filter then
# applied one Process's protocol to a different Process's Services.
#
# A Process can have no Service 1 whenever Service 1 is removed, because
# "service_id" is allocated from a counter that only increases.
#
# To Do
# ~~~~~
# - None, yet !

from aiko_services.main.dashboard import DashboardFrame
from aiko_services.main.service import ServiceTopicPath

PIPELINE = "github.com/geekscape/aiko_services/protocol/pipeline:0"
ELEMENT = "github.com/geekscape/aiko_services/protocol/pipeline_element:0"
ACTOR = "github.com/geekscape/aiko_services/protocol/actor:0"

PROCESS_A = "aiko/host/100"
PROCESS_B = "aiko/host/200"


def _service(topic_path, protocol):
    # Only fields 0 and 2 are read by the filter
    return (topic_path, "name", protocol, "transport", "owner")


class _Dashboard:
    # The three methods under test read "self.filter_out" and call
    # "self._short_name()", and nothing else, so they run without a Screen or
    # a Frame. Taken from DashboardFrame rather than copied, so this stub
    # cannot drift away from the code it stands in for
    _short_name = DashboardFrame._short_name
    _protocol_base = DashboardFrame._protocol_base
    _service_parents = DashboardFrame._service_parents
    _filter = DashboardFrame._filter

    def __init__(self, filter_out):
        self.filter_out = set(filter_out)


def _dashboard(filter_out):
    return _Dashboard(filter_out)


def _shown(dashboard, services):
    # Mirror the live Services loop: collect the parents once, so the answer
    # does not depend on where a Service 1 sits in the list
    parents = dashboard._service_parents(services)
    shown = []
    for service in services:
        topic_path = ServiceTopicPath.parse(service[0])
        protocol = dashboard._short_name(service[2])
        if dashboard._filter(parents, topic_path, protocol):
            shown.append(service[0])
    return shown


def _shown_history(dashboard, services):
    # Mirror the history loop: collect the parents while walking, so a Service
    # is judged by the Service 1 of its own Process most recently seen before
    # it. Returns indexes, because the history repeats a topic path
    parents = {}
    shown = []
    for index, service in enumerate(services):
        topic_path = ServiceTopicPath.parse(service[0])
        protocol = dashboard._short_name(service[2])
        if topic_path.service_id == "1":
            parents[topic_path.topic_path_process] =  \
                dashboard._protocol_base(protocol)
        if dashboard._filter(parents, topic_path, protocol):
            shown.append(index)
    return shown


def test_the_pipeline_element_filter_still_hides_its_own_pipelines_elements():
    # Control: if this fails, the filter is broken outright and the test below
    # proves nothing
    dashboard = _dashboard(["pipeline_element"])
    services = [
        _service(f"{PROCESS_A}/1", PIPELINE),
        _service(f"{PROCESS_A}/2", ELEMENT),
    ]

    assert _shown(dashboard, services) == [f"{PROCESS_A}/1"]


def test_a_process_without_service_id_1_is_not_given_another_process_parent():
    # Process B has no Service 1, so it has no parent and no Pipeline, and its
    # Services must not be hidden by Process A being a Pipeline
    dashboard = _dashboard(["pipeline_element"])
    services = [
        _service(f"{PROCESS_A}/1", PIPELINE),
        _service(f"{PROCESS_A}/2", ELEMENT),
        _service(f"{PROCESS_B}/2", ACTOR),  # Service 1 was removed
    ]

    assert f"{PROCESS_B}/2" in _shown(dashboard, services), \
        "a Process with no Service 1 inherited another Process's parent"


def test_the_result_does_not_depend_on_the_order_of_the_services():
    # The same three Services, with Process B first. If the parent is carried
    # through the list rather than held per Process, these two orders disagree,
    # which is the defect stated as a property
    dashboard = _dashboard(["pipeline_element"])
    a1 = _service(f"{PROCESS_A}/1", PIPELINE)
    a2 = _service(f"{PROCESS_A}/2", ELEMENT)
    b2 = _service(f"{PROCESS_B}/2", ACTOR)

    assert sorted(_shown(dashboard, [a1, a2, b2])) == \
           sorted(_shown(dashboard, [b2, a1, a2]))


def test_an_empty_service_list_has_no_parents_and_shows_nothing():
    # The Dashboard runs before any Service is registered, and the parent pass
    # must not assume the list has anything in it
    dashboard = _dashboard(["pipeline_element"])

    assert dashboard._service_parents([]) == {}
    assert _shown(dashboard, []) == []


def test_both_passes_reduce_a_protocol_the_same_way():
    # The parent pass stores a reduced protocol and the filter reduces the one
    # it is given. If those two ever disagree, a Pipeline stops being
    # recognised as a parent and the filter silently stops working
    dashboard = _dashboard([])
    service = _service(f"{PROCESS_A}/1", PIPELINE)

    parents = dashboard._service_parents([service])
    assert parents[PROCESS_A] ==  \
        dashboard._protocol_base(dashboard._short_name(service[2]))


def test_history_judges_each_process_lifetime_by_its_own_service_1():
    # A Process key is "namespace/hostname/pid" and the history outlives a
    # Process, so one key holds more than one Process once an operating system
    # reuses a process id. Here pid 100 is an Actor Process, and later a
    # Pipeline Process. Only the Services of the Pipeline lifetime may be
    # hidden by the "pipeline_element" filter. A parent collected for the whole
    # list instead of while walking it reaches back over the earlier lifetime
    # and hides row 1 as well
    dashboard = _dashboard(["pipeline_element"])
    history = [
        _service(f"{PROCESS_A}/1", ACTOR),     # the Actor Process
        _service(f"{PROCESS_A}/2", ACTOR),     # its Service 2, must be shown
        _service(f"{PROCESS_A}/1", PIPELINE),  # the Process that reused the id
        _service(f"{PROCESS_A}/2", ACTOR),     # its Service 2, must be hidden
    ]

    assert _shown_history(dashboard, history) == [0, 1, 2], \
        "a later Process on a reused id changed the parent of an earlier one"


def test_history_still_scopes_a_parent_to_its_own_process():
    # The defect this change fixes, in the history view: Process B has no
    # Service 1, so it has no parent and must not take the parent of Process A
    dashboard = _dashboard(["pipeline_element"])
    history = [
        _service(f"{PROCESS_A}/1", PIPELINE),
        _service(f"{PROCESS_B}/2", ACTOR),
    ]

    assert 1 in _shown_history(dashboard, history), \
        "a Process with no Service 1 inherited another Process's parent"


def test_service_1_is_never_hidden_by_the_sid_filter():
    dashboard = _dashboard(["sid>1"])
    services = [
        _service(f"{PROCESS_A}/1", PIPELINE),
        _service(f"{PROCESS_A}/2", ELEMENT),
    ]

    assert _shown(dashboard, services) == [f"{PROCESS_A}/1"]


def test_a_protocol_in_filter_out_is_hidden_whatever_its_parent():
    dashboard = _dashboard(["actor"])
    services = [
        _service(f"{PROCESS_B}/2", ACTOR),
    ]

    assert _shown(dashboard, services) == []

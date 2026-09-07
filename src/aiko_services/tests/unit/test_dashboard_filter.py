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


def test_where_a_service_1_sits_does_not_change_the_answer():
    # The parent of a Process is collected before any Service is filtered, so
    # Service 1 does not have to arrive before the Services of its own Process
    dashboard = _dashboard(["pipeline_element"])
    parent = _service(f"{PROCESS_A}/1", PIPELINE)
    element = _service(f"{PROCESS_A}/2", ACTOR)

    assert dashboard._service_parents([parent, element]) ==  \
           dashboard._service_parents([element, parent])


def test_two_processes_on_one_id_have_no_parent_either_way():
    # The history keeps the Services of Processes that have stopped, so one
    # "namespace/hostname/pid" key can hold more than one Process once an
    # operating system reuses a process id. Two Service 1 Services under one
    # key is less knowledge than one, not more, so the parent is unknown, and
    # the Services are shown rather than hidden on the strength of which
    # Service 1 happened to come last. "share.py" writes the history from both
    # ends, so that order is not defined and must not decide what is displayed
    dashboard = _dashboard(["pipeline_element"])
    actor_1 = _service(f"{PROCESS_A}/1", ACTOR)
    pipeline_1 = _service(f"{PROCESS_A}/1", PIPELINE)
    element = _service(f"{PROCESS_A}/2", ACTOR)

    forwards = [actor_1, pipeline_1, element]
    backwards = [pipeline_1, actor_1, element]

    assert dashboard._service_parents(forwards) == {PROCESS_A: None}
    assert dashboard._service_parents(backwards) == {PROCESS_A: None}
    assert f"{PROCESS_A}/2" in _shown(dashboard, forwards)
    assert f"{PROCESS_A}/2" in _shown(dashboard, backwards)


def test_one_process_repeating_its_service_1_still_has_a_parent():
    # Only a DIFFERENT protocol means two Processes. The same Service 1 listed
    # twice is one Process, and its Services must still be filtered
    dashboard = _dashboard(["pipeline_element"])
    pipeline_1 = _service(f"{PROCESS_A}/1", PIPELINE)
    element = _service(f"{PROCESS_A}/2", ACTOR)

    services = [pipeline_1, pipeline_1, element]

    assert dashboard._service_parents(services) == {PROCESS_A: "pipeline"}
    assert f"{PROCESS_A}/2" not in _shown(dashboard, services)


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

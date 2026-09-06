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

import types

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


def _dashboard(filter_out):
    # The filter reads "self.filter_out" and calls "self._short_name()", and
    # nothing else, so it runs without a Screen or a Frame
    return types.SimpleNamespace(
        filter_out=set(filter_out),
        _short_name=DashboardFrame._short_name.__get__(object()),
    )


def _shown(dashboard, services):
    # Mirror the render loops: build the parents once, then filter each Service
    parents = DashboardFrame._service_parents(dashboard, services)
    shown = []
    for service in services:
        topic_path = ServiceTopicPath.parse(service[0])
        protocol = dashboard._short_name(service[2])
        if DashboardFrame._filter(dashboard, parents, topic_path, protocol):
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

# Usage
# ~~~~~
# pytest [-s] unit/test_services_iterator.py
# pytest [-s] unit/test_services_iterator.py::test_a_comprehension_over_services
#
# Regression tests for "ServicesIterator" in "service.py", which defined
# "__next__" without "__iter__", so anything calling "iter()" on the iterator
# that "Services.__iter__" returns stopped with a TypeError.
#
# Two things hide the fault, and these tests are shaped around both. A "for"
# statement uses the iterator directly and passes with the fault present, so
# "test_a_for_statement_over_services" was green before the fix and is kept to
# record that. An empty "Services" also hides it, because "Services.__iter__"
# returns "iter([])" when it holds no Service.
#
# To Do
# ~~~~~
# - None, yet !

from aiko_services.main.service import Services

ACTOR = "github.com/geekscape/aiko_services/protocol/actor:0"
PROCESS = "namespace/hostname/100"

def _services(count=2):
    services = Services()
    for service_id in range(1, count + 1):
        topic_path = f"{PROCESS}/{service_id}"
        services.add_service(
            topic_path, [topic_path, "name", ACTOR, "owner", []])
    return services

def test_a_for_statement_over_services():
    count = 0
    for service in _services():
        count += 1
    assert count == 2

def test_a_comprehension_over_services():
    assert len([service for service in _services()]) == 2

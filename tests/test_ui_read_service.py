from types import MappingProxyType

from src.cloud.ui_read_service import _json_safe


def test_json_safe_converts_mappingproxy_metadata() -> None:
    value = MappingProxyType({
        "nested": MappingProxyType({"role": "FDE"}),
        "items": (MappingProxyType({"tree": "entity:user"}),),
    })

    assert _json_safe(value) == {
        "nested": {"role": "FDE"},
        "items": [{"tree": "entity:user"}],
    }

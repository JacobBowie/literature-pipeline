"""Fix review of 9c87a4b (gate): reproductions written at 5487c3c."""
import pytest

from litpipe import gate
from tests.test_w5_verify_r import b1_rows, spec


# ---------------------------------------------------------------- FR-6: a nested field skips the plain-name rule
@pytest.mark.parametrize("tpl", ["{cited_by:{chapters[0]}}", "{cited_by:{topics[0]}}", "{order:{year.real}}"])
def test_fr6_a_nested_field_with_index_or_attribute_access_is_refused_at_load(tpl):
    with pytest.raises(gate.SpecError, match="plain field names"):
        gate.load_spec(spec(output={"notes_template": tpl}))



def test_fr6c_nested_plain_fields_and_format_specs_still_load():
    for tpl in ("o={order:03d}; t={assigned_topic}", "{title!r}" if "title" in gate.NOTES_FIELDS else "{gate!r}",
                "{{literal}} {gate}", "{cited_by:>{n_seeds}}"):
        gate.load_spec(spec(output={"notes_template": tpl}))


# ---------------------------------------------------------------- FR-R2: the TypeError catch has no lock
def test_fr_r2_a_type_error_in_the_sample_format_is_a_spec_error():
    """Passes at 5487c3c (the TypeError clause catches it); fails when that clause is reverted (mutation R2);
    passes with FR-6 (the nested field is refused by name first)."""
    with pytest.raises(gate.SpecError):
        gate.load_spec(spec(output={"notes_template": "{gate:{n_seeds[0]}}"}))

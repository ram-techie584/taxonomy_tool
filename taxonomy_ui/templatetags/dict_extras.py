from django import template

register = template.Library()

@register.filter
def get_value(row, key):
    if isinstance(row, dict):
        return row.get(key, "")
    return getattr(row, key, "")

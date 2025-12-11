# taxonomy_ui/templatetags/custom_filters.py
from django import template

register = template.Library()

@register.filter(name='get_item')
def get_item(dictionary, key):
    """Template filter to get dictionary item by key"""
    if isinstance(dictionary, dict):
        return dictionary.get(key, "")
    return ""
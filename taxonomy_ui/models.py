# taxonomy_ui/models.py
from django.db import models


class PartMaster(models.Model):
    part_number = models.CharField(max_length=100)
    updated_at = models.DateTimeField(auto_now=True)

    dimensions = models.CharField(max_length=255, blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    cost = models.CharField(max_length=100, blank=True, null=True)
    material = models.CharField(max_length=255, blank=True, null=True)
    vendor_name = models.CharField(max_length=255, blank=True, null=True)
    currency = models.CharField(max_length=50, blank=True, null=True)

    category_raw = models.CharField(max_length=255, blank=True, null=True)
    category_master = models.CharField(max_length=255, blank=True, null=True)

    source_system = models.CharField(max_length=50, blank=True, null=True)
    source_file = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        db_table = "part_master"          # <- Django must use this table name
        ordering = ["part_number"]

    def __str__(self):
        return f"{self.part_number} - {self.description or ''}"

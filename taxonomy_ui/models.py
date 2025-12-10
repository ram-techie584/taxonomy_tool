#from django.db import models

# Create your models here.
# taxonomy_ui/models.py

from django.db import models


class PartMaster(models.Model):
    id = models.AutoField(primary_key=True)
    part_number = models.CharField(max_length=100)
    updated_at = models.DateTimeField()
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
        managed = False  # Django will NOT create/alter this table
        db_table = "part_master"
        ordering = ["part_number"]

    def __str__(self):
        return f"{self.part_number} - {self.description or ''}"

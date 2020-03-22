#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test Cases:
  Is the primary key in this table unique?
  Is [important column name] free of null values?
  Do these foreign keys exist as primary keys in their referenced tables (referential integrity)?
  Does the numeric data in this table fall within an expected range?
  Do item-level purchase amounts sum to order-level amounts?
  Do the necessary columns for external work exist in the final table?
  Can we preemptively run popular dashboard queries from our business intelligence tool?
"""
import os
import pytest

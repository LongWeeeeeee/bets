#!/usr/bin/env python3
print("1")
import sys
print("2")
sys.path.insert(0, '.')
print("3")
from core.transitive_analyzer import TransitiveAnalyzer
print("4")
analyzer = TransitiveAnalyzer()
print(f"5: {len(analyzer.matches_sorted)} matches")

"""Retrieval evaluation harness.

Measures search quality (nDCG@10, MRR, recall@10) over a fixed query set with
LLM-judged, arm-agnostic relevance grades, so any retrieval change -- a
reranker, a new embedding model -- can be measured before/after rather than
swapped on intuition. See eval/README.md for the methodology.
"""

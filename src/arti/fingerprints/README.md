# Fingerprint

A Fingerprint represents a unique identity as an int64 value.

Using an int(64) has a number of convenient properties:
- can be combined independent of order with XOR
- can be stored relatively cheaply
- empty 0 values drop out when combined (5 ^ 0 = 5)
- is relatively cross-platform (across databases, languages, etc)

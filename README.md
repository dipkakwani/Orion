# Causal Key Value Store

Built on top of DKVF (https://github.com/roohitavaf/DKVF)

CLAP - Causal Look Ahead Partition

TACC - Time Approximated Causal Consistency

## TODO

1. ~~Implement ROT for Causal Spartan in DKVF~~
   1. ~~Algorithm described in the paper - CausalSpartanX: Causal Consistency and Non-Blocking Read-Only Transactions https://arxiv.org/pdf/1812.07123.pdf~~
   2. ~~ROT implementation available in MSUDB https://github.com/roohitavaf/MSUDB~~
2. ~~Check multiple versions~~
3. Calculate relative time difference in client and DSV from server
4. Approximate DSV at client side
5. Handle wrong approximation cases
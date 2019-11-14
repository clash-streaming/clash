## CLASH Validation

Place the validation files into this directory. These files are not checked in into the repository.
Ask Manuel on how to get them.


## Run validations

The CLASH main executable has a validation command included. Run

```bash
$ clash validation
```

in order to see all available validation queries. Run

```bash
$ clash validation tpchq2j
``` 

to run the **tpch** query **q2** which computes only **joins** for validation.


## Test list

* **rst1**: This is a three-way join, `r.x = s.x and s.y = t.y`, where the following combinations are tested:
  * joins with single partners work (x, y = 1 .. 3)
  * joins with totally different values don't work (x, y = 11...18)
  * two-way success does not mean three-way (x, y = 21..26 for R,S; x, y = 31..36 for S,T)
  * joins with multiple join partners work (x, y = 41..43 for R(1:n)S(1:n)T; x, y = 51..53 for R(n:1)S(n:1)T,)

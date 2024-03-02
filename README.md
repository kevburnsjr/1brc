[WSL] First pass:
```
> time ./main > /dev/null

real    0m5.825s
user    0m5.094s
sys     0m3.714s
```
[WSL] After adopting `customStringToIntParser`: (WSL)
```
> time ./main > /dev/null

real    0m4.291s
user    0m0.329s
sys     0m3.465s
```
[Native]
```
$ time ./main.exe > /dev/null

real    0m2.877s
user    0m0.000s
sys     0m0.000s
```

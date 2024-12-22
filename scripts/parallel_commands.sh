#shuf commands.txt | parallel --jobs 1 "taskset -c 48 sh -c {}"
shuf commands.txt | parallel --jobs 24 "taskset -c \$(( {%} + 23 )) sh -c {}"
#shuf commands.txt | parallel --jobs 48 "taskset -c {%} sh -c {}"
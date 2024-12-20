#cat commands.txt | parallel --jobs 1 "taskset -c 56 sh -c {}"
cat commands.txt | parallel --jobs 7 "taskset -c \$(( {%} *  8 )) sh -c {}"
#cat commands.txt | parallel --jobs 56 "taskset -c {%} sh -c {}"
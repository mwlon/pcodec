cat commands.txt | parallel --jobs 7 "echo taskset -c \$(( 8 + {%} *  8 )) {}"

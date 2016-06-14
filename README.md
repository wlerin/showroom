# showroom
Requires Requests, Pytz, and a working installation of ffmpeg


Syntax:

To watch and download from one member's page:

    python3 showroom.py "Member Name"


To watch all pages and download up to MAX_DOWNLOADS (default: 20) at once:

    python3 showroom.py --all [<output directory>]


For additional options:

    python3 showroom.py --help 



### Install Required Packages

    pip3 install pytz requests
    


### INDEX PRIORITIES

First and most importantly, lower numbers == more important.
Then, a member with a lower priority can trump one with a higher priority, if that higher priority is double or more
e.g. A priority 1 member takes precedence over priority 2 and above
while a priority 2 member takes precedence over priority 4 and above.

If there are no open watch or download slots (see MAX_WATCHES and MAX_DOWNLOADS)
but a low priority member is waiting for one, that member might bump a high priority member 
completely off, killing any ongoing downloads and removing watches from the queue

Be very careful who you give low priorities to, especially ones less than 10 (the default is 20)

# aws-stats

A very simple program for analyzing the S3 log format. It's very, very
alpha, but you're welcome to use the code as you like.

## Usage

```
lein run /path/to/dir/with/log/files
```

Which produces a report that looks like this: 

```
                                   object   requests equivalent downloads
                      004-aaron-bedra.jpg       5176  5140.9355
                005-michael-parenteau.jpg       5005   4990.602
              REL_logo_square_300x300.jpg       1104  523.02856
  ThinkRelevance-003-brenton-ashworth.mp3       1095  531.31335
       ThinkRelevance-004-aaron-bedra.mp3        564  356.94485
 ThinkRelevance-005-michael-parenteau.mp3        520   326.5346
      ThinkRelevance-002-david-liebke.mp3        346  223.93367
    ThinkRelevance-001-JustinGehtland.mp3        278  180.68216
```

`Requests` is the raw number of requests for the object that got a 200
back. `Equivalent downloads1 is the number of bytes transferred
divided by the object size. So two half-downloads count as a whole
one. I wanted that figure because it's possible to get multiple hits
for a single .mp3 if people are downloading in chunks. Not perfect,
but the best I could do quickly.

I use [s3cmd](http://s3tools.org/s3cmd) to sync the log files to a
local directory.

## License

Copyright (C) 2012 Craig Andera

Distributed under the Eclipse Public License, the same as Clojure.

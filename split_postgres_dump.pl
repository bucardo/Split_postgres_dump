#!/usr/bin/env perl

## Break a Postgres dump file into pre, data, and post files
## This allows you to load the initial schema,
## populate the data as a separate step,
## and then apply the indexes, constraints, and triggers
##
## Greg Sabino Mullane <greg@endpoint.com>
## Copyright End Point Corporation 2009-2010
## BSD licensed
## See: http://bucardo.org/wiki/Split_postgres_dump

## Usage: pg_dumpall --schema-only > dumpfile; split_postgres_dump.pl dumpfile

use strict;
use warnings;
use Data::Dumper;
use 5.006000;

our $VERSION = '1.3.3';

my $USAGE = qq{Usage: $0 dumpfile\n};

my $file = shift or die $USAGE;

if ($file =~ /\-+version/i) {
	print "$0 version $VERSION\n";
	exit 0;
}


-e $file or die qq{No such file: $file\n};

open my $fh, '<', $file or die qq{Could not open "$file": $!\n};

if (<$fh> !~ /^\Q--\E$/
	or <$fh> !~ /^\-\- PostgreSQL database (?:cluster )?dump$/
	or <$fh> !~ /^\Q--\E$/)
	{
		die qq{File "$file" does not appear to be a Postgres dump file\n};
	}

my $olines = 3;

(my $prefile = $file) .= '.predata';
open my $afh, '>', $prefile or die qq{Could not open "$prefile": $!\n};
print {$afh} "--\n-- PostgreSQL database dump (pre-data)\n--\n";
my $prelines = 3;

(my $datafile = $file) .= '.dataonly';
open my $dfh, '>', $datafile or die qq{Could not open "$datafile": $!\n};
print {$dfh} "--\n-- PostgreSQL database dump (data only)\n--\n";
my $datalines = 3;

(my $postfile = $file) .= '.postdata';
open my $zfh, '>', $postfile or die qq{Could not open "$postfile": $!\n};
print {$zfh} "--\n-- PostgreSQL database dump (post-data)\n--\n";
my $postlines = 3;

my $lastline = '';
my $mode = 'pre';
my $lastdb = '?';
my $inside_func = 0;
while (<$fh>) {

	$olines++;

	if ($inside_func) {
		if ($_ eq "\$_\$;\n") {
			$inside_func = 0;
		}
	}
	else {

		if ($lastline =~ /^    LANGUAGE \w+$/o
			and index($_,'    AS $_$')==0
			) {
			$inside_func = 1;
		}

		## New database connection goes to all, and flips to 'pre' mode
		if (/^\\connect (.+)/o) {
			$lastdb = $1;
			$mode = 'pre';
			print {$afh} $_; $prelines++;
			print {$dfh} $_; $datalines++;
			print {$zfh} "$_\n"; $postlines+=2;
			next;
		}

		## SET strings always go to all
		if (/^SET \w/o) {
			print {$afh} $_; $prelines++;
			print {$dfh} $_; $prelines++;
			print {$zfh} $_; $postlines++;
			next;
		}

		## End of a database gets written to all, changes mode to 'end'
		if (/^\Q-- PostgreSQL database dump complete\E$/o) {

			if ('post' eq $mode) {
				print {$afh} "--\n"; $prelines++;
				print {$dfh} "--\n"; $prelines++;
			}
			print {$afh} "-- PostgreSQL database dump complete (pre-data) DB=$lastdb\n--\n\n";
			$prelines++;

			print {$dfh} "-- PostgreSQL database dump complete (data) DB=$lastdb\n--\n\n";
			$datalines++;

			print {$zfh} "-- PostgreSQL database dump complete (post-data) DB=$lastdb\n";
			$postlines++;

			$mode = 'end';
			next;
		}
	}

	## If in 'pre' mode, check for anything that might end it
	## If found, write it only to second file and switch to 'post' or 'data' mode
	if ('pre' eq $mode) {

		## We end if we see an ALTER TABLE ONLY (just perms has no ONLY)
		if (!$inside_func and /^ALTER TABLE ONLY/o) {
			$mode = 'post';
			print {$zfh} $_;
			$postlines += 2;
		}
		## A new index or rule indicates we need to switch to 'post' mode
		elsif (/^CREATE (?:UNIQUE )?(?:INDEX|RULE).+/ and !$inside_func) {
			$mode = 'post';
			print {$zfh} $_;
			$postlines+=2;
		}
		elsif (/^COPY .+ FROM stdin/ and !$inside_func) {
			$mode = 'data';
			print {$dfh} $_;
			$datalines+=1;
		}
		## Default for pre mode is to simply print to the first file
		else {
			print {$afh} $_; $prelines++;
		}

		## Store the last line in case we need it (e.g. ADD CONSTRAINT above)
		$lastline = $_;
		next;
	}

	if ('data' eq $mode) {
		print {$dfh} $_; $datalines++;
		if (/^\\\.$/) {
			print {$dfh} "\n"; $datalines++;
			$mode = 'pre';
		}
		next;
	}

	if ('post' eq $mode) {
		print {$zfh} $_; $postlines++;
		$lastline = $_;
		next;
	}

	if ('end' eq $mode) {
		print {$zfh} $_; $postlines++;
		next;
	}

	die "Invalid mode: $mode\n";

}

close $fh or die qq{Could not close "$file": $!\n};
close $afh or die qq{Could not close "$prefile": $!\n};
close $dfh or die qq{Could not close "$datafile": $!\n};
close $zfh or die qq{Could not close "$postfile": $!\n};

my $maxfile = length $postfile;
printf qq{Lines in original file  %-*s : %d\n}, $maxfile, $file, $olines;
printf qq{Lines in pre-data file  %-*s : %d\n}, $maxfile, $prefile, $prelines;
printf qq{Lines in data-only file %-*s : %d\n}, $maxfile, $datafile, $datalines;
printf qq{Lines in post-data file %-*s : %d\n}, $maxfile, $postfile, $postlines;

exit;

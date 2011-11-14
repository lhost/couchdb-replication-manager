#!/usr/bin/perl -w

#
# couchdb-replication-manager.pl
#
# Developed by lhost 
# Copyright (c) 2011 lhost
# Licensed under terms of GNU General Public License.
# All rights reserved.
#
# Changelog:
# 2011-10-29 - created
#

use strict;
use threads;
use threads::shared;
#use Thread::Queue;

$| = 1;

use Env;
use English;
use POSIX qw( WNOHANG EAGAIN );
use POSIX qw( strftime );
use Time::HiRes qw( gettimeofday tv_interval );
use JSON::XS qw( encode_json decode_json );
use LWP::UserAgent;
use HTTP::Request::Common;
use HTTP::Headers;
use Getopt::Long;
use Net::HTTP;
use MIME::Base64;
use Sys::Hostname;
use Config::IniFiles;
use File::Basename;
use Data::Dumper;
#use AnyEvent::CouchDB;

use vars qw (
	$VERSION $DEBUG
	$opt_config $opt_schema $opt_heartbeat $opt_feed $opt_interval
	$couchdb_dsn $username $password $couch
	$opt_print_help
	$proto $host $port $dbname $auth_header
	$db_status $response
	$hostname
	$conf $schema_config
);

$hostname = hostname();
my $schema_changed :shared;
$schema_changed = 0;

$VERSION	= sprintf("%d", q$Revision: 7667 $ =~ /(\d+)/);
$DEBUG		= 0;

sub get_all_tasks($$);
sub thread_couchdb_changes();
sub create_db($);
sub print_info($;@);
sub cur_date(;$);
sub help();

my $rv = GetOptions(
	'config|c=s'	=> \$opt_config,
	'schema|s=s'	=> \$opt_schema,
	'heartbeat=i'	=> \$opt_heartbeat,
	'feed=s'		=> \$opt_feed,
	'interval=i'	=> \$opt_interval,
	'dsn|d=s'		=> \$couchdb_dsn,
	'debug'			=> \$DEBUG,
	'help|h'		=> \$opt_print_help,
	
);

my $defaultconfig;
my $cfg_ini;
if (-r '/etc/couchdb/couchdb-replication-manager.ini') {
	$defaultconfig = Config::IniFiles->new(-file => '/etc/couchdb/couchdb-replication-manager.ini');
}
if (defined($opt_config) and ! -f $opt_config) {
	die "Config file '$opt_config' not found: $!";
}
else {
	foreach my $f ($opt_config, './couchdb-replication-manager.ini', "$ENV{HOME}/.couchdb/couchdb-replication-manager.ini") {
		if (defined($f) and -f $f) {
			print_info "Reading configuration from file '$f'";
			if (defined($defaultconfig) and ref($defaultconfig eq 'Config::IniFiles') ) {
				$cfg_ini = Config::IniFiles->new( -file => $f, -import $defaultconfig);
			}
			else {
				$cfg_ini = Config::IniFiles->new( -file => $f);
			}
			print Dumper($cfg_ini);
			die Dumper( [ @Config::IniFiles::errors ] ) unless $cfg_ini;
			last;
		}
		$cfg_ini ||= $defaultconfig;
	}
}

$opt_schema		||= $cfg_ini->val('replicator', 'schema');
$opt_heartbeat	||= $cfg_ini->val('replicator', 'heartbeat')	|| 2000;
$opt_feed		||= $cfg_ini->val('replicator', 'feed')			|| 'continuous';
$opt_interval	||= $cfg_ini->val('replicator', 'interval')		|| 10;
$couchdb_dsn	||= $cfg_ini->val('couchdb', 'dsn');
$proto			||= $cfg_ini->val('couchdb', 'proto')			|| 'http';
$host			||= $cfg_ini->val('couchdb', 'server')			|| 'localhost';
$port			||= $cfg_ini->val('couchdb', 'port')			|| 5984;
$dbname			||= $cfg_ini->val('couchdb', 'database')		|| 'replication-manager';
$username		||= $cfg_ini->val('couchdb', 'username');
$password		||= $cfg_ini->val('couchdb', 'password');

if ($opt_feed eq 'continuous') {
}
else {
	die "Unsupported feed type '$opt_feed'";
}

if ($opt_print_help) {
	help();
}

unless (defined $opt_schema and $opt_schema ne '') {
	die "Replication schema undefined. Parameter --schema <ID-of-your-rep-schema-document> required";
}

# CouchDB DSN parsing
if (defined($couchdb_dsn) and $couchdb_dsn =~ m|^([^:]+)://(.*):?([^:]*)/([^/]+)|) { # {{{
	$proto	= $1;
	$host	= $2;
	$port	= $3;
	$dbname	= $4;
	if (!defined($port) or $port eq '') {
		if ($proto eq 'http') {
			$port = 80;
		}
		elsif ($proto eq 'https') {
			$port = 443;
		}
		else {
			die "Undefined protocol '$proto' in CouchDB DSN '$couchdb_dsn'";
		}
	}
	elsif ($port !~ m/^\d+$/) {
		die "Invalid port '$port' in  CouchDB DSN '$couchdb_dsn'";
	}
}
elsif (defined $couchdb_dsn) {
	die "CouchDB DSN '$couchdb_dsn' doesn't match pattern http://some.server.com[:port]/database";
} # }}}

if ($host =~ s/^([^\@]*)\@//) {  # Auth {{{ get rid of potential "user:pass@"
	if (!defined($username) and !defined($password)) {
		$auth_header = "Basic " . encode_base64($1);
		#warn Dumper($auth_header);
	}
}
else {
	$auth_header = "Basic " . encode_base64("$username:$password") if (defined($username) and defined($password));
} # }}}

$couchdb_dsn ||= "$proto://$username:$password\@$host/$dbname";

my $ua = LWP::UserAgent->new;
#$ua->timeout(10);

# Get DB info {{{
$response = $ua->get($couchdb_dsn);
if ($response->is_success) {
	#print Dumper( decode_json($response->decoded_content) );
	$db_status =  decode_json($response->decoded_content);
}
else {
	print_info "Error opening database '$dbname': ", $response->status_line;
	my $rsp = decode_json($response->decoded_content);
	#warn Dumper( $rsp );
	if (defined($rsp) and exists($rsp->{error}) and $rsp->{error} eq 'not_found') {
		if (exists($rsp->{reason}) and $rsp->{reason} eq 'no_db_file') {
			create_db($couchdb_dsn) or print_info "Error creating db '$dbname'";
		}
	}
} # }}}

# Get schema info {{{
$response = $ua->get("$couchdb_dsn/$opt_schema");
if ($response->is_success) {
	$schema_config =  decode_json($response->decoded_content);
	#print Dumper( $schema_config );
}
else {
	print_info $response->status_line;
	die Dumper( decode_json($response->decoded_content) );
} # }}}

# create nodes hash
foreach my $node (@{$schema_config->{nodes}}) {
	$conf->{nodes}->{$node->{name}}		= $node->{server};
}
# create list of jobs for this node
foreach my $j (@{$schema_config->{replication}->{links}}) { # {{{
	my ($from, $to, $type) = @$j;
	if ($type eq 'both') { # {{{
		if ($hostname eq $from or $hostname eq $to) {
			foreach my $db (@{$schema_config->{replication}->{databases}}) {
				push @{$conf->{jobs}}, {
					from	=> $from,
					to		=> $to,
					db		=> $db,
				} if ($hostname eq $from);
				push @{$conf->{jobs}}, {
					from	=> $to,
					to		=> $from,
					db		=> $db,
				} if ($hostname eq $to);
			}
		}
	} # }}}
	elsif ($type eq 'push') { # {{{
		if ($hostname eq $from) {
			foreach my $db (@{$schema_config->{replication}->{databases}}) {
				push @{$conf->{jobs}}, {
					from	=> $from,
					to		=> $to,
					db		=> $db,
				};
			}
		}
	} # }}}
}
# }}}

#warn Dumper($conf->{jobs});

my $t = threads->create('thread_couchdb_changes');
$t->detach();

#sleep 2; print Dumper({ threads => [threads->list(threads::all)] });

threads->yield();

while (1) {
	my $tasks_running = get_all_tasks($ua, "$proto://$username:$password\@$host");
	#warn Dumper($tasks_running);

	foreach my $j (@{$conf->{jobs}}) {
		if (exists($tasks_running->{ $j->{db} })
				and exists($tasks_running->{ $j->{db} }->{ $conf->{nodes}->{ $j->{to} } })
				and $tasks_running->{ $j->{db} }->{ $conf->{nodes}->{ $j->{to} } }->{type} eq 'Replication') {
			print_info "Replication of db '$j->{db}' --> '$j->{to}' is running, skipping..." if ($DEBUG);
			next;
		}
		my $response = $ua->post("$proto://$username:$password\@$host/_replicate", 
			'Content-Type'	=> 'application/json',
			'Referer'		=> "$proto://$host/_utils/replicator.html",
			'Content'	=> encode_json( {
					'source' => $j->{db},
					'target' => "$proto://$username:$password\@$conf->{nodes}->{ $j->{to} }/$j->{db}",
					'continuous'	=> JSON::XS::true})
		);
		if ($response->is_success) {
			my $rsp = decode_json($response->decoded_content);
			#warn Dumper($rsp);
			if (defined($rsp) and exists($rsp->{ok}) and $rsp->{ok} == 1) {
				print_info "Replication of db '$j->{db}' --> '$j->{to}' started as job '$rsp->{_local_id}'";
			}
			else {
				if (defined($rsp) and exists($rsp->{error}) and $rsp->{error} eq 'not_found') {
					if (exists($rsp->{reason}) and $rsp->{reason} eq 'no_db_file') {
						create_db("$proto://$username:$password\@$conf->{nodes}->{ $j->{to} }/$j->{db}") or print_info "Error creating db '$j->{db}'";
					}
				}
				print_info "Error replicating db '$j->{db}' --> '$j->{to}': ", Dumper($rsp);
			}
		}
		else {
			create_db("$proto://$username:$password\@$conf->{nodes}->{ $j->{to} }/$j->{db}") or print_info "Error creating db '$j->{db}'";
			print_info "Error replicating db '$j->{db}' --> '$j->{to}': ", $response->status_line;
			#warn Dumper($response->decoded_content);
			#warn Dumper(decode_json($response->decoded_content));
		}

	}

	if ($schema_changed) { # {{{
		print_info "Restarting...";
		sleep $opt_interval;
		exit 0;
	} # }}}
	sleep $opt_interval;
}

exit 0;

sub get_all_tasks($$)
{ # {{{
	my ($ua, $src) = @_;

	my $response = $ua->get("$src/_active_tasks");
	if ($response->is_success) {
		my $rsp = decode_json($response->decoded_content);
		my $r;
		foreach my $t (@$rsp) {
			if ($t->{task} =~ m|^([^:]+):\s+(\S+)\s+->\s+([^:]+)://([^:]*):?(\S*?)@?([^@]+)/([^/]+)/$|) {
				my $x = $r->{$2}->{$6} = { };
				foreach my $k (qw( type pid task status )) {
					$x->{$k} = $t->{$k};
				}
				$x->{local_id_short}	= $1;
				$x->{src_db}			= $2;
				$x->{proto}				= $3;
				$x->{dst_user}			= $4;
				$x->{dst_password}		= $5;
				$x->{dst_host}			= $6;
				$x->{dst_db}			= $7;
			}
		}
		return $r;
	}
	else {
		return undef;
	}
} # }}}

# watch for changes on $opt_schema document
sub thread_couchdb_changes()
{ # {{{
	#warn Dumper($auth_header);
	print_info "Watching for changes on '$opt_schema' document";

	my $s = Net::HTTP->new(Host => $host) || die $@;
	# "$couchdb_dsn/_changes"
	# "$couchdb_dsn/_changes?feed=longpoll&heartbeat=10000"
	# "$couchdb_dsn/_changes?feed=continuous&since=$db_status->{update_seq}&filter=replication-manager/rep-schema
	$s->write_request(GET => "/$dbname/_changes?feed=$opt_feed&heartbeat=$opt_heartbeat&since=$db_status->{update_seq}",
		($auth_header ? (Authorization => $auth_header) : undef)
	);

	if ($opt_feed eq 'continuous') { # {{{
		while (1) {
			my $buf;
			my $n = $s->read_entity_body($buf, 1024);
			die "read failed: $!" unless defined $n;
			last unless $n;
			#warn Dumper({data => $buf });
			my $jrsp;
			if ($buf =~ m/^(\d+)\r\n(.+)\n\r\n$/) {
				eval { $jrsp = decode_json($2); };
				#warn Dumper($jrsp);
				if ($@) {
					warn "Invalid JSON '$2': $@";
				}
				else {
					#warn Dumper($jrsp);
					if ($jrsp->{id} eq $opt_schema) {
						$schema_changed++;
						print_info "Replication schema change detected, reload needed. id = '$jrsp->{id}',  _rev = '$jrsp->{changes}->[0]->{rev}'";
					}
				}
			}
		}

	} # }}}
	else {
		die "Unsupported feed type '$opt_feed'";
	}

	$schema_changed++;
	warn "thread exit";

} # }}}

sub create_db($)
{ # {{{
	my ($db) = @_;
	my $rsp2 = $ua->request(
		HTTP::Request::Common::PUT(
			$db, Content	=> ''
		)
	);
	unless ($rsp2->is_success) {
		print_info "Error creating db '$db': ", Dumper($rsp2);
	}
} # }}}

sub print_info($;@)
{ # {{{
	my ($info, @text) = @_;
	my $progname = basename($0);
	printf "%s [%s] %s: %s", cur_date(), $PID, $progname, $info;
	print @text if (scalar(@text) > 0);
	print "\r\n";
} # }}}

sub cur_date(;$)
{ # {{{
	my $append_msec  = shift;
	my ($sec, $msec) = Time::HiRes::gettimeofday;
	my $time_string  = strftime("%Y-%m-%d %T", localtime);
	if (defined($append_msec) && $append_msec) {
		$time_string .= sprintf('.%04d', $msec / 1E2);
	}
	return $time_string;
} # }}}

sub help()
{ # {{{
	print <<EOF;
couchdb-replication-manager.pl   Revision: $VERSION

	This tool can be used to build CouchDB cluster. Replication in your cluster
	is described by 'rep-schema' documents stored in CouchDB database
	'replication-manager'. Database 'replication-manager' is replicated to all
	nodes and couchdb-replication-manager.pl is running on every node of this
	cluster. couchdb-replication-manager.pl read document with ID
	<ID-of-your-rep-schema-document> from CouchDB and manages jobs related to local
	node.

	You should start this tool from init by adding the following line into your /etc/initttab:

		cm:2345:respawn:/path/to/couchdb-replication-manager.pl --schema <ID-of-your-rep-schema-document>

EOF
	exit 1;

} # }}}

# vim: ts=4
# vim600: fdm=marker fdl=0 fdc=3


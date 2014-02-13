#!/usr/bin/perl

# DGAS urcollector for Condor
# 
# 2013-04-11 - David Rebatto <david.rebatto@mi.infn.it>
#

use strict;
use vars qw($dbh $insert_query %config_values $log);
use POSIX qw(setsid);
use DBI;
use Log::Dispatch;
use Log::Dispatch::File;
use Getopt::Long;

my $version = '0.3beta';
my $config_file = "/etc/dgas/dgas_sensors.conf";
my $max_iterations = 0;
my $nodaemon = 0;

# Get command line options
&GetOptions(
  "nodaemon"     => \$nodaemon, 
  "iterations=i" => \$max_iterations,
  "config=s" =>     \$config_file,
  # For backward compatibility, an unqualified argument
  # is also assumed to be the configuration file
  "<>" => sub { $config_file = shift; },
) or die "Bad options specified";

$log = Log::Dispatch->new(
  callbacks => sub { 
                 my %p = @_; 
                 return localtime() .
                        " [" . $$ . "]" .
                        "[" . $p{level} . "] " . 
                        $p{message};
               },
);

# Configuration loading
# ---------------------
sub load_config {

  my $config_file = shift;
  my $FATAL = 0;

  # Set some default values
  %config_values = (
    condorAcctLogDir =>  join(",", map({ "/var/lib/condor/spool/" . $_ . "/history" } ( qw(atlas test short) ))),
    condor_history_command => "/usr/bin/condor_history",
    ce_endpoint => "t2-ce-04.mi.infn.it:8443/cream-condor-",
    vo_filter => "atlas",
    legacy_cmd => "/usr/libexec/dgas-atmClient",
    dgasDB => "/tmp/dgas_testdb.sqlite",
    systemLogLevel => "notice",
    collectorLogFileName => "/var/log/dgas/dgas_urcollector.log"
  );

  open( CONFIGFILE, $config_file ) or die("Error: Cannot open configuration file '$config_file'");
  while (<CONFIGFILE>) {
    next if (/^\s*#/) or (/^\s*$/); # Skip comments and empty lines
    if (/^\s*([^\s]*)\s*=\s*"(.*)"/) {
      my $key = $1;
      my $value = $2;
      $config_values{$key} = ( $value =~ /\$\{(.*)\}/ ) ? $ENV{$1} : $value;
    } else {
      die ("bad config file") if $FATAL;
    }
  }
  close CONFIGFILE;
  if ( $config_values{systemLogLevel} =~ /\d+/ ) {
    # Convert the number in a Log::Dispatch compatible log level
    # (0=debug, 7=emergency)
    $config_values{systemLogLevel} = 9 - $config_values{systemLogLevel};
    $config_values{systemLogLevel} = 7 if $config_values{systemLogLevel} > 7;
  }

  # If we already had a log object we are reconfiguring,
  # otherwise we are starting up for the first time
  my $reconfiguring = $log->remove('LOG');

  # Set the new log output object
  $log->add(
    Log::Dispatch::File->new(
      name      => "LOG",
      min_level => $config_values{systemLogLevel},
      filename  => $config_values{collectorLogFileName},
      mode      => '>>',
      newline   => 1,
      close_after_write => 1,
    )
  );

  if ( not defined $reconfiguring ) {
    $log->notice(qq|--- STARTUP ---
*********************************************
** DGAS urcollector for Condor starting up
** $0
** Version = $version
** PID file = $config_values{collectorLockFileName}
** Config = $config_file
** Log level = $config_values{systemLogLevel}
** Iterations limit = $max_iterations (0=unlimited)
*********************************************|
    );
  } else {
    $log->notice("configuration reloaded from $config_file");
    $log->info("log level is now [$config_values{systemLogLevel}]");
  }
}

load_config $config_file;

# Create the lock file
my $lockfile = $config_values{collectorLockFileName};
if ( -e $lockfile ) {
  $log->critical("Lock file $lockfile exists, closing this instance");
  die("Lock file exists");
}
open LOCKFILE, "> $lockfile" or $log->log_and_die(
    level => "critical",
    message => "Can't open lock file $lockfile"
  );

# Daemonize self unless --nodaemon is specified
# Could have used Proc::Daemon but it's not installed
# by default, and I don't want to add a dependency for
# just a couple of lines...
if ( $nodaemon ) {
  print LOCKFILE $$;
  close LOCKFILE;
} else {
  defined ( my $pid = fork() )
    or $log->log_and_die(
      level => "critical",
      message => "Can't fork daemon: $!"
    );
  if ($pid) {
    # Father: write the lock file and exit
    $log->info("daemon forked with pid = $pid");
    print LOCKFILE $pid;
    close LOCKFILE;
    exit 0;
  }

  # If we get here, this is the child process
  close LOCKFILE;
  chdir '/';
  open STDIN, '/dev/null' or die "Can't read /dev/null: $!";
  open STDOUT, '>/dev/null' or die "Can't write to /dev/null: $!";
  open STDERR, '>/dev/null' or die "Can't write to /dev/null: $!";
  setsid;
} 


{ # Condor block - Limit the scope of static variables

  # Define the -format for history command, as close as possible to the final syntax.
  # The fields that depend on a classAd value but cannot be formatted with 
  # classAd functions are replaced with a place_holder of the format 
  # %%<TYPE>_<CLASSADVALUE>%% and handled afterwards according to <TYPE>.
  my $history_format =
    q|-format "--jobid \"%s\" "                   'GridJobId =?= UNDEFINED ? GlobalJobId : GridJobId' |.
    q|-format "--usrcert \"%s\" "                 x509userproxysubject |.
    q|-format "--time %s "                        QDate |. 
    # We can provide only the queue here, need to add the CE endpoint from ldif
    q|-format "--resgridid \"%%%%RESID_%s%%%%\" " 'regexps("^([^#]*)", GlobalJobId, "\1")' |.
    # Condor uses ',' as separator for FQANs, it must be replaced with ';'
    # (I havent' found a way to do that with classAd functions)
    q|-format "\"fqan=%%%%FQAN_%s%%%%\" "         x509UserProxyFQAN |.
    q|-format "\"QUEUE=%s\" "                     'regexps("^([^#]*)", GlobalJobId, "\1")' |.
    q|-format "\"USER=%s\" "                      Owner |.
    q|-format "\"userVo=%s\" "                    x509UserProxyVOName |.
    # The group cannot be retrieved from classad, retrieve it later
    q|-format "\"group=%%%%GROUP_%s%%%%\" "       Owner |.
    q|-format "\"CPU_TIME=%d\" "                  'int(RemoteSysCpu + RemoteUserCpu)' |.
    q|-format "\"WALL_TIME=%d\" "                 'int(RemoteWallClockTime)' |.
    q|-format "\"PMEM=%d\" "                      ResidentSetSize_RAW |.
    q|-format "\"VMEM=%d\" "                      ImageSize_RAW |.
    q|-format "\"LRMSID=%s\" "                    GlobalJobId |.
    q|-format "\"jobName=%s\" "                   Cmd |.
    q|-format "\"execHost=%s\" "                  LastRemoteHost |.
    q|-format "\"execCe=%%%%RESID_%s%%%%\" "      'regexps("^([^#]*)", GlobalJobId, "\1")' |.
    q|-format "\"lrmsServer=%%%%CM_%s%%%%\" "     'regexps("^([^#]*)", GlobalJobId, "\1")' |.
    q|-format "\"start=%s\" "                     JobStartDate |.
    q|-format "\"end=%s\" "                       EnteredCurrentStatus |.
    q|-format "\"ctime=%s\" "                     QDate |.
    q|-format "\"qtime=%s\" "                     QDate |.
    q|-format "\"etime=%s\" "                     QDate |.
    q|-format "\"exitStatus=%s\" "                ExitStatus |.
    q|-format "\"URCREATION=%s\" "                'formatTime(CurrentTime)' |.
    # These values are stored in the classad at submission time
    # via SUBMIT_EXPRS in the condor configuration file
    q|-format "\"accountingProcedure=%s\" "       '(JobType =?= "grid") ? "grid" : "outOfBand"' |.
    q|-format "\"ceCertificateSubject=%s\" "      ceCertificateSubject |.
    q|-format "\"SiteName=%s\" "                  SiteName |.
    # These values don't really belong to the "Args" field.
    # They're here just to pass information to the UR manager without
    # writing more parsing code (take profit of place_holder hooks).
    q|-format "%%%%STARTTIME_%s%%%%"              JobStartDate |.
    q|-format "%%%%JOBID_%s%%%%"                  GlobalJobId |.
    # These values are precached and don't depend on any classad value
    # (no need to escape the '%' when no actual value is passed)
    q|-format "|.
    q|\"voOrigin=fqan\" |.
    q|\"si2k=%%GlueHostBenchmarkSI00%%\" |.
    q|\"sf2k=%%GlueHostBenchmarkSF00%%\" |.
    q|\"GlueCEInfoTotalCPUs=%%GlueSubClusterLogicalCPUs%%\" |.
    q|\n" EMPTY|;
  
  my %translate = (); # The cache for the substitution values
  my %last_job_id = (); # Remember last job for each schedd
  if ( open(MARK, "< $config_values{collectorBufferFileName}") ) {
    while (<MARK>) {
      my ($key, $value) = split;
      $last_job_id{$key} = $value;
      $log->info("bookmark loaded for $key: $value");
    }
  }

  # Cache initialisation
  # --------------------
  # N.B. Configuration must have been already loaded
  sub init_cache {
    $log->notice("initialising cache...");
    %translate = ();

    # Precache some values from ldap
    my @ldap_keys = split /,/, $config_values{keyList};
    my $ldap_cmd = q|ldapsearch -LLL -h localhost -p 2170 -x -b "o=grid" |.
                   q|"(objectClass=GlueHostBenchmark)" |.
                   join(" ", @ldap_keys);
    if (open(LDAP, "$ldap_cmd |")) {
      while (<LDAP>) {
        my ( $key, $value ) = split /: /;
        if ( $key ~~ @ldap_keys ) {
          chomp $value;
          $translate{$key} = $value;
          $log->debug("cashed $key => $translate{$key}");
        }
      }
      close LDAP;
    }
    else {
      $log->error("error executing $ldap_cmd");
    }
    $log->notice("cache initialised");
  }


  # The actual scan function
  # ------------------------
  sub scan_condor_history {
    my ($notbefore, $ce_name_filter, $vo_filter) = @_;
    my $record_count = 0;

    foreach my $schedd_dir (split /:/, $config_values{condorAcctLogDir}) {
      $log->info("examining $schedd_dir");
      # Build the specific command for the current schedd
      my $command = qq|$config_values{condor_history_command} -backward |.
                    qq|$history_format -f "$schedd_dir/history"|;
    
      # Add constraints
      {
        my @constraint;
        if ($notbefore) {
          push @constraint, qq|(EnteredCurrentStatus > $notbefore)|;
        }
        if ($ce_name_filter) {
          push @constraint, qq|RegExp(\\"CN=$ce_name_filter\\", ceCertificateSubject)|;
        }
        if ($vo_filter) {
          push @constraint, qq|(x509UserProxyVOName == \\"$vo_filter\\")|
        }
	# [...] need to add startdate and stopdate constraints?

        if (@constraint) {
          $command .= q| -constraint "| . join(" && ", @constraint) . q|"|;
        }
      }
      $log->debug("executing command:\n$command");

      # Retrieve the job history
      unless (open(JOBHIST, "$command |")) {
        $log->error("Error executing $command");
        # History files are NFS mounted so don't give up,
        # maybe next time the file will be readable
        next;
      }
    
      # Elaborate the records
      my $first_job_id = "";
      LINE: while (my $jobrecord = <JOBHIST>) {
    
        my $job_id;
        my $start_time;
  
        # If the UR contains any %%place_holder%%
        # replace it with the proper value, 
        # caching when possible
        $log->debug("processing line\n$jobrecord");
        while ($jobrecord =~ /%%([^%]*)%%/) {
          my $placeholder = $1;
          my $retval = "";

          # If the value is not cached, compute it
          unless (exists $translate{$placeholder}) {
            ($_, my $value) = split(/_/, $placeholder);
            SWITCH: {
              /^GROUP$/ && do {
                $retval = `id -gn $value`;
                last SWITCH;
              };
              /^RESID$/ && do {
                $retval = $config_values{ce_endpoint} . $value;
                last SWITCH;
              };
              /^FQAN$/ && do { 
                # drop the first fqan (not a VOMS role) 
                # and replace commas with semicolons
                my @fqans = split /,/, $value;
                shift @fqans;
                $retval = join ';', @fqans;
                last SWITCH;
              };
              /^CM$/ && do {
                # get the machine where the schedd is running
                $retval = `condor_status -schedd "$value" -format "%s" Machine`;
                last SWITCH;
              };
  
              # The next patterns intentionally set no $retval to completely
              # remove the dummy info from "Args" field
              /^STARTTIME$/ && do {
                $start_time = $value;
                last SWITCH;
              };
              /^JOBID$/ && do {
                $job_id = $value;
                $first_job_id = $job_id if $first_job_id eq "";
                last SWITCH;
              };
            } # end SWITCH
            # Store the value in the cache hash
            if ($retval) {
              chomp $retval;
              $translate{$placeholder} = $retval
            }
          } else {
            # Use the cached value
            $log->debug("using cached value $translate{$placeholder} for $placeholder");
            $retval = $translate{$placeholder};
          }
    
          $jobrecord =~ s/%%$placeholder%%/$retval/ ;
        } # end of placeholder substitution

        # If we have already seen this job, go to the next schedd
        if ($last_job_id{$schedd_dir} eq $job_id) {
          $log->info("bookmark $job_id reached for $schedd_dir, skipping the rest...");
          close JOBHIST;
          last LINE;
        }

        # Spit out the record
        chomp $jobrecord;
        process_UR($job_id, $jobrecord, $start_time);
        $record_count += 1;
      } 
      close JOBHIST;
      $last_job_id{$schedd_dir} = $first_job_id if $first_job_id;
    } # next schedd
    # save the last_job_id hash
    if ( open MARK, "> $config_values{collectorBufferFileName}" ) {
      while ( my ($key, $value) = each %last_job_id ) {
        print MARK "$key $value\n";
        $log->debug("bookmarked $value as last job for $key");
      }
      close MARK;
    } else {
      $log->warning("error while opening bookmark file: $!");
    }

    return $record_count;
  }


} # End of Condor specific block
# ------------------------------


sub process_UR {

  my ($jobid, $args, $start_time) = @_;
  my $retry_count = 0;

  $log->debug("inserting job $jobid in the database");
  until ($insert_query->execute($args, $start_time, $jobid)) {
    $log->error($dbh->err . " while inserting job $jobid");
    if (++$retry_count <= 3 && $dbh->err != 19) {
      sleep int(rand 3)+1;
      $log->warning("retrying insertion");
    } else {
      $log->error("record not inserted");
      last;
    }
  }
}


# MAIN
# ----

init_cache;

# Prepare the DB stuff
$dbh = DBI->connect("dbi:SQLite:$config_values{dgasDB}")
  or $log->log_and_die(
    level => "critical",
    message => "Cannot connect: $DBI::errstr"
  );
$dbh->{AutoCommit} = 0;
$dbh->{RaiseError} = 0;
# TABLE commands definition:
#  key INTEGER PRIMARY KEY, 
#  transport TEXT KEY, 
#  composer TEXT, 
#  arguments TEXT, 
#  producer TEXT, 
#  recordDate TEXT, 
#  lrmsId TEXT, 
#  commandStatus INT
$insert_query = $dbh->prepare(
  "INSERT INTO commands ".
  "VALUES (NULL, 'Legacy', '$config_values{legacy_cmd}', ?, '', ?, ?, '')"
) or $log->log_and_die(
  level => "critical",
  message => "Can't prepare statement: $DBI::errstr"
);

# N.B. these variables are accessible to locally defined anonymous functions
# (e.g. the signal handlers)
my $notbefore = 0;
my $got_sigterm = 0;
my $reset_cache = 0;
my $reload_config = 0;
my $cycle_number = 0;
my $record_count = 0;

# Set signal handlers
$SIG{TERM} = sub { $got_sigterm = 1 };
$SIG{INT} = sub { $got_sigterm = 2 };
$SIG{QUIT} = sub { $got_sigterm = 3 };
# Some LRMS specific sensors could miss one of these functions,
# check before setting the signal handler.
$SIG{HUP} = sub { $reload_config = 1; } if defined &load_config;
$SIG{USR1} = sub { $reset_cache = 1; } if defined &init_cache;

# Keep feeding the database
# -------------------------
# No conditions here as they whould be evaluated after the 'sleep'
while (1) {
  $cycle_number += 1;

  # SIGALRM is used only during the 'sleep'
  $SIG{ALRM} = 'IGNORE';

  $log->notice("Starting cycle $cycle_number");
  $record_count = scan_condor_history($notbefore, "t2-ce-04.mi.infn.it", $config_values{vo_filter});
  $log->notice("found $record_count new records");
  if ($record_count > 0) {
    $dbh->commit;
    if ($dbh->err) {
      $dbh->rollback;
      $log->log_and_die(
        level => "critical",
        message => "Can't commit to database: $DBI::errstr"
      );
    }
    $log->notice("records committed to database");
  }

  if ($max_iterations > 0 && $cycle_number >= $max_iterations) {
    $log->notice("iterations limit $max_iterations reached, program exiting.");
    last;
  }

  # Allow the 'sleep' to wake up with SIGALRM
  $SIG{ALRM} = sub { $log->warning("SIGALRM received, forcing next iteration")};
  $log->notice("cycle $cycle_number terminated, going to sleep for $config_values{mainPollInterval} seconds");
  sleep $config_values{mainPollInterval}
    unless ($reset_cache or $reload_config or $got_sigterm);

  if ($got_sigterm) {
    $log->warning(qw(SIGTERM SIGINT SIGQUIT)[$got_sigterm-1] . " received, program exiting.");
    last;
  }
  if ($reset_cache) {
    $log->warning("SIGUSR1 received, clearing cached values");
    init_cache;
    $reset_cache = 0;
  }
  if ($reload_config) {
    $log->warning("SIGHUP received, closing logfile and reloading configuration");
    load_config $config_file;
    $reload_config = 0;
  }  
}
unlink $config_values{collectorLockFileName} if (not $nodaemon);
$log->notice("Goodbye!");

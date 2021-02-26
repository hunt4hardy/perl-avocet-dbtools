package Avocet::dbtools;

use 5.00506;
use strict;
use warnings;
use Time::localtime;

require Exporter;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);
@ISA = qw(Exporter);

our %EXPORT_TAGS = (
          'all' => [ qw(
                          date_now
                          db_connect
                          callproc
                          single_select
                          single_select_bind
                          parse_login
                  ) ],
);

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );


@EXPORT = qw(
	
);

$VERSION = '1.04';


# -------------------------------------------------------------------------
# --- open_db_connection ---
# -------------------------------------------------------------------------
sub db_connect
{
   my ($conn_file) = @_;
 
   my $db = parse_login($conn_file);

   # --- connect to database ---
   my $dbh = DBI->connect("dbi:Pg:database=$db->{dbase_base};
                                      host=$db->{dbase_host};
                                      port=$db->{dbase_port}",
                          $db->{dbase_user},
                          $db->{dbase_pass}
                         ) || die 'Database Connection Failed...';
   
   print "Connected to database: $db->{dbase_host} - $db->{dbase_base}\n";
   return $dbh;
}
   

# ------------------------------------------------------------------------
#  callproc()
# ------------------------------------------------------------------------
sub callproc
{
   my ($dbh, $function, @params) = @_;
   #print "Executing function $function\n";

   # --- check db connection has been made ---
   if (! defined $dbh)
   {
      print $function . "(): No DB connection found...\n";
      exit;
   }

   my $sql;

   # --- construct stored proc call with parameter place holders ---
   my $call = $function . "(";

   my $cnt=1;
   while ($cnt <= scalar @params)
   {
      $call .= "?";
      if ($cnt < scalar(@params)) 
      {
         $call .= ", ";
      }
      $cnt++;
   }
   $call .= ")";

   # --- construct select to call the stored proc ---
   $sql = "SELECT id, ok, err 
             FROM $call
               AS (id  int, 
                   ok  int, 
                   err character varying);";
   #print "sql: $sql\n";


   my $stmt = $dbh->prepare($sql);
   
   # --- bind variables ---
   $cnt = 1;
   while ($cnt <= scalar @params)
   {
      #print "binding $cnt to: " . $params[$cnt-1] . "\n";
      $stmt->bind_param($cnt,$params[$cnt-1]);
      $cnt++;
   }
   $stmt->execute;

   # --- check for sql error ---
   if ($DBI::err)
   {
      if ($function ne 'bill_save_batch_log')
      {
         &main::log_msg($DBI::errstr);
      }
      else
      {
         print $DBI::errstr . "\n";
      }
      exit;
   }
   
   my $result = $stmt->fetchrow_hashref();
   
   # --- check for stored proc error ---
   if ($result->{ok} eq '0')
   {
      if ($function ne 'bill_save_batch_log')
      {
         &main::log_msg($function . "(): ERROR - " . $result->{err});
      }
      else
      {
         print $function . "(): ERROR - " . $result->{err} . "\n";
      }
      exit;
   }
   
   undef $stmt;
   return $result;
}

# ------------------------------------------------------------------------
#  single_select()
# ------------------------------------------------------------------------
sub single_select
{
   my ($dbh, $sql) = @_;

   my $stmt = $dbh->prepare($sql);
   $stmt->execute;

   my $row = $stmt->fetchrow_hashref;
   undef $stmt;
   return $row;
}

# ------------------------------------------------------------------------
#  single_select_bind()
# ------------------------------------------------------------------------
sub single_select_bind
{
   my ($dbh, $sql, @bind_array) = @_; 
   my $stmt = $dbh->prepare($sql);

   my $i = 1;
   foreach my $bind (@bind_array)
   {   
      $stmt->bind_param($i,$bind_array[$i-1]);
      $i++;
   }   
   $stmt->execute;

   my $row = $stmt->fetchrow_hashref;
   undef $stmt;
   return $row;
}



# -------------------------------------------------------------------------
# --- current date ---
# -------------------------------------------------------------------------
sub date_now
{
   my ($l_format) = @_;

   # --- current date ---
   my $l_year = localtime->year() + 1900;
   my $l_mon  = localtime->mon();
   my $l_mday = localtime->mday();
   my $l_months = "JanFebMarAprMayJunJulAugSepOctNovDec";
   my $l_mth = substr($l_months,$l_mon*3,3);
   my $l_hour = localtime->hour();
   my $l_min = localtime->min();
   my $l_sec = localtime->sec();

   # --- dd-mmm-yyyy hh:mm:ss ---
   if ($l_format == 1)
   {
      return sprintf "%02d-%s-%d %02d:%02d:%02d",
                       $l_mday,$l_mth,$l_year,$l_hour,$l_min,$l_sec;
   }

   # --- yyyy_mm_dd_hhmmss ---
   elsif ($l_format == 2)
   {
      return sprintf "%04d_%02d_%02d_%02d%02d%02d",
                       $l_year, $l_mon+1, $l_mday,$l_hour,$l_min,$l_sec;
   }

   # --- dd-mmm-yyyy ---
   elsif ($l_format == 3)
   {
      return sprintf "%02d-%s-%d",
                       $l_mday,$l_mth,$l_year;
   }
}

# ------------------------------------------------------------------------
#  parse_login()
# ------------------------------------------------------------------------
sub parse_login
{
   my ($conn_file) = @_;

   # --- check file exists ---
   if (! -e $conn_file)
   {
      print "parse_login(): File not found: $conn_file\n";
      exit(0);
   }

   # --- Get credentials from file ---
   open my $fh, '<', $conn_file;
   my @array = <$fh>;
   chomp @array;
   close $fh;

   # --- look at each line ---
   my $record;
   foreach my $line (@array)
   {
      # --- filter comments ---
      if (($line =~ /^#/) || (($line !~ /^([^=]*)=([^=]*)$/) && 
                              ($line !~ /^([^=]*)=\"(.*)\"$/)))
      {
         #print "Ignoring: $line\n";
         next;
      }

      # --- store name value pairs ---
      $record->{$1} = $2;
   }

   return $record;
}


1;

__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

Avocet::dbtools - Perl extension for Avocet

=head1 SYNOPSIS

  use Avocet::dbtools;
  This is a collection of tools primarily to assist calls to DBI and
  the Avocet postgres database

=head1 DESCRIPTION

This is a collection of tools primarily to assist calls to DBI and
the Avocet postgres database


=head2 EXPORT

None by default.



=head1 SEE ALSO


=head1 AUTHOR

Simon Allen, Avocet

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2016 by Simon Allen (Avocet)

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.18.2 or,
at your option, any later version of Perl 5 you may have available.


=cut


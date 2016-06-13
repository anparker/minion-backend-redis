package Minion::Backend::Redis;
use Mojo::Base 'Minion::Backend';

use Mojo::JSON qw(decode_json encode_json);
use Mojo::Redis2;
use Sys::Hostname 'hostname';
use Time::HiRes 'time';

our $VERSION = '0.1';

has name => 'ns';
has 'redis';

my @FIELDS = (
  qw(attempts args created queue id priority task state retries),
  qw(delayed worker started finished retried result)
);

sub dequeue {
  my ($self, $id, $wait, $options) = @_;

  my $redis = $self->redis;
  my $queues = $options->{queues} //= ['default'];
  $wait ||= 1;

  # check deps
  $self->_resolve_deps($_, 'parents', 1)
    for @{$redis->smembers($self->_key('pending'))};

  # enqueue delayed jobs
  my $dkey = $self->_key('delayed');
  $self->_enqueue(split ':', $_) and $redis->zrem($dkey, $_)
    for @{$redis->zrangebyscore($dkey, 0, time)};

  # wait for items from queue
  my $job_id
    = $redis->brpop((map $self->_q($_), @$queues), int($wait + 0.5))->[1];
  return unless $job_id && $redis->exists(my $key = $self->_j($job_id));

  $redis->sadd($self->_w($id => 'jobs'), $job_id);

  # update job
  $redis->hmset($key, started => time, state => 'active', worker => $id);

  # and fetch info
  my $job;
  @{$job}{qw(id args retries task)}
    = @{$redis->hmget($key, qw(id args retries task))};
  $job->{args} = decode_json $job->{args};

  return $job;
}

sub enqueue {
  my ($self, $task, $args, $options) = (shift, shift, shift || [], shift || {});

  $options->{attempts} //= 1;
  my $delayed = time + ($options->{delay} //= 0);
  my $priority = $options->{priority} //= 0;
  my $queue    = $options->{queue}    //= 'default';
  my $id       = $self->_job_id;

  my $redis = $self->redis;
  my $key   = $self->_j($id);

  # add queue
  $redis->sadd($self->_key('queues'), $queue);

  $redis->sadd($self->_q($queue => 'jobs'), $id);

  my $parents = delete $options->{parents};

  # create job
  $redis->hmset(
    $key, %$options,
    args    => encode_json($args),
    created => time,
    delayed => $delayed,
    id      => $id,
    retries => 0,
    state   => 'inactive',
    task    => $task,
  );

  # our happy family x_x
  if ($parents) {
    $redis->sadd("$key.parents", @$parents);
    $redis->sadd($self->_j($_ => 'children'), $id) for @$parents;
    $redis->sadd($self->_key('pending'), $id);
  }

  # ...always alone
  else {
    $self->_enqueue($queue, $id, $priority, ($options->{delay} ? $delayed : 0));
  }

  return $id;
}

sub fail_job { shift->_update(1, @_) }

sub finish_job { shift->_update(0, @_) }

sub job_info {
  my ($self, $id) = @_;

  my $redis = $self->redis;

  return undef unless $id && $redis->exists(my $key = $self->_j($id));

  my $job;
  @{$job}{@FIELDS} = @{$redis->hmget($key, @FIELDS)};
  $job->{args}     = decode_json $job->{args};
  $job->{result}   = decode_json $job->{result} // 'null';
  $job->{parents}  = $redis->smembers("$key.parents");
  $job->{children} = $redis->smembers("$key.children");

  return $job;
}

sub list_jobs {
  my ($self, $offset, $limit, $options) = @_;

  my $redis = $self->redis;

  my $queue
    = $options->{queue}
    ? [$options->{queue}]
    : $redis->smembers($self->_key('queues'));

  my @ids;
  push @ids, @{$redis->smembers($self->_q($_ => 'jobs'))} for @$queue;

  my @list = sort { $b->{id} <=> $a->{id} } grep {
          (!$options->{state} || ($options->{state} eq $_->{state}))
      and (!$options->{task}  || ($options->{task} eq $_->{task}))
  } map $self->job_info($_), @ids;

  return [splice @list, ($offset // 0), ($limit // ~0 >> 1)];
}

sub list_workers {
  my ($self, $offset, $limit) = @_;

  my @ids
    = sort { $b <=> $a } @{$self->redis->smembers($self->_key('workers'))};
  @ids = splice @ids, ($offset // 0), ($limit // ~0 >> 1);
  return [map $self->worker_info($_), @ids];
}

sub new {
  return shift->SUPER::new(redis => Mojo::Redis2->new(@_));
}

sub register_worker {
  my ($self, $id) = @_;

  my $redis = $self->redis;
  my $key = $self->_w($id //= $self->_worker_id);

  $redis->hset($key, notified => time) and return $id if $redis->exists($key);

  $redis->sadd($self->_key('workers'), $id);
  $redis->hmset(
    $key,
    id       => $id,
    host     => ($self->{host} //= hostname),
    pid      => $$,
    started  => time,
    notified => time,
  );

  return $id;
}

sub remove_job {
  my ($self, $id) = @_;

  my $redis = $self->redis;
  my $key   = $self->_j($id);

  my ($state, $queue) = @{$redis->hmget($key, qw(state queue))};
  return undef if !$state || $state eq 'active';

  my $qkey = $self->_q($queue);
  $redis->lrem($qkey, 1, $id)
    or $redis->srem($self->_key('pending'), $id)
    or $redis->zrem($self->_key('delayed'), $id);
  $redis->del($key, "$key.parents", "$key.children");
  $redis->srem("$qkey.jobs", $id);
}

sub repair {
  my $self = shift;

  my $minion = $self->minion;
  my $redis  = $self->redis;

  # Workers without heartbeat
  my $missing_after = time - $minion->missing_after;
  my %workers = map { $_ => 1 } @{$redis->smembers($self->_key('workers'))};
  for (keys %workers) {
    my $notified = $redis->hget($self->_w($_), 'notified');
    delete $workers{$_} && $self->unregister_worker($_)
      if $notified < $missing_after;
  }

  my $queues = $redis->smembers($self->_key('queues'));

  for my $q (@$queues) {

    my $key = $self->_q($q => 'jobs');
  NEXT_JOB:
    for (@{$redis->scan->smembers($key)}) {
      my ($id, $retries, $state, $finished, $worker)
        = @{$redis->hmget($self->_j($_), qw(id retries state finished worker))};

      # Jobs without definition (can't do anything about them)
      $redis->srem($key, $_) and next unless $id;

      # Jobs with missing worker (can be retried)
      if ($state eq 'active' && !$workers{$worker}) {
        $self->fail_job($id, $retries, 'Worker went away');
        next;
      }

      # Jobs with missing parents (can't be retried)
      if ($state eq 'inactive') {
        !$redis->exists($self->_j($_))
          and do { $self->_fail_job($id, 'Parent went away'); next NEXT_JOB; }
          for @{$redis->smembers($self->_j($id => 'parents'))};
      }

      # Old jobs with no unresolved dependencies
      $self->remove_job($id)
        if $state eq 'finished'
        && $finished < time - $minion->remove_after
        && $self->_resolve_deps($id, 'children');
    }
  }
}

sub reset {
  my $self = shift;

  my $redis = $self->redis;
  my $keys  = $redis->scan->keys($self->_key('*'));
  $redis->del(@$keys) if @$keys;
}

sub retry_job {
  my ($self, $id, $retries, $options) = (shift, shift, shift, shift || {});

  my $redis = $self->redis;

  my $job
    = $redis->hmget(my $key = $self->_j($id), qw(state retries priority queue));
  return unless $job->[0] && $job->[1] == $retries && $job->[0] ne 'active';

  my $queue    = $options->{queue}    // $job->[3];
  my $priority = $options->{priority} // $job->[2];
  my $delayed = time + ($options->{delay} //= 0);

  $self->_enqueue($queue, $id, $priority, ($options->{delay} ? $delayed : 0));
  $redis->smove($self->_q($job->[3] => 'jobs'),
    $self->_q($queue => 'jobs'), $id)
    if $options->{queue};

  $redis->hmset(
    $key,
    delayed  => $delayed,
    priority => $priority,
    queue    => $queue,
    retried  => time,
    retries  => ($job->[1] + 1),
    state    => 'inactive',
  );
}

sub stats {
  my $self  = shift;
  my $redis = $self->redis;

  my $stats;
  for my $q (@{$redis->smembers($self->_key('queues'))}) {
    for my $job_id (@{$redis->scan->smembers($self->_q($q => 'jobs'))}) {
      my $job = $redis->hmget($self->_j($job_id), 'state', 'worker');
      $stats->{"${$job}[0]_jobs"}++;
      $stats->{active_workers}{$job->[1]} = 1 if $job->[0] eq 'active';
    }
  }
  $stats->{delayed_jobs}   = $redis->zcard($self->_key('delayed'));
  $stats->{active_workers} = keys %{$stats->{active_workers}};
  $stats->{inactive_workers}
    = $redis->scard($self->_key('workers')) - $stats->{active_workers};
  $stats->{"${_}_jobs"} ||= 0 for qw(inactive active failed finished);

  return $stats;
}

sub unregister_worker {
  my ($self, $id) = @_;

  my $redis = $self->redis;

  my $key = $self->_w($id);
  $redis->del($key, "$key.jobs");
  $redis->srem($self->_key('workers'), $id);
}

sub worker_info {
  my ($self, $id) = @_;

  my $redis = $self->redis;

  return unless $redis->exists(my $key = $self->_w($id //= ''));

  my $worker;
  @{$worker}{qw(id host pid started notified)}
    = @{$redis->hmget($key, qw(id host pid started notified))};
  $worker->{jobs} = $redis->smembers("$key.jobs");
  return $worker;
}

sub _enqueue {
  my ($self, $queue, $id, $priority, $delayed) = @_;

  my $redis = $self->redis;
  my $key   = $self->_q($queue);

  # delay job if needed
  return $redis->zadd($self->_key('delayed'), $delayed, "$queue:$id:$priority")
    if $delayed;

  # get last added jobs with same or higher priority
  my %prio = @{$redis->hgetall("$key.priorities")};
  my @last = sort { $b <=> $a } grep { $_ >= $priority } keys %prio;

  my $done;

# in a context of the list above, insert a job after lowest priority one still
# enqueued
# (cause list'll be processed in inverse order, inserting with 'before' keyword)
  $redis->linsert($key, 'before', $prio{pop @last}, $id) > 0
    and do { $done = 1; last; }
    for 1 .. @last;

  # ... or push it, if neither found
  $redis->rpush($key, $id) unless $done;

  # store last added job
  $redis->hset("$key.priorities", $priority, $id);

  return $self;
}

sub _fail_job {
  my ($self, $id, $result) = @_;

  $self->redis->hmset(
    $self->_j($id),
    finished => time,
    result   => encode_json($result),
    state    => 'failed',
  );
}

sub _j { shift->_key(job => @_) }

sub _job_id { return shift->redis->incr('minion.last_job_id') }

sub _key {
  return join '.', 'minion', shift->name, @_;
}

sub _q { shift->_key(queue => @_) }

sub _resolve_deps {
  my ($self, $id, $relation, $enq) = @_;

  my $redis = $self->redis;

  my $resolved = 1;
  my $dependencies = $redis->smembers($self->_j($id => $relation));
  ($redis->hget($self->_j($_), 'state') || '') ne 'finished'
    and do { $resolved = 0; last; }
    for @$dependencies;

  if ($resolved && $enq) {
    my $job = $redis->hmget($self->_j($id), qw(queue priority delayed));

    $self->_enqueue(shift @$job, $id, @$job);
    $redis->srem($self->_key('pending'), $id);
  }

  return $resolved;
}

sub _update {
  my ($self, $fail, $id, $retries, $result) = @_;
  my $redis = $self->redis;

  my $job = $redis->hmget(my $key = $self->_j($id),
    qw(state retries attempts worker));

  return undef
    unless $job->[0] && $job->[0] eq 'active' && $job->[1] == ($retries //= 0);

  my $state = $fail ? 'failed' : 'finished';
  $redis->hmset(
    $key,
    finished => time,
    result   => encode_json($result),
    state    => $state,
  );

  $redis->srem($self->_w($job->[3] => 'jobs'), $id);

  return 1 if !$fail || (my $attempts = $job->[2]) == 1;
  return 1 if $retries >= ($attempts - 1);
  my $delay = $self->minion->backoff->($retries);
  return $self->retry_job($id, $retries, {delay => $delay});
}

sub _w { shift->_key(worker => @_) }

sub _worker_id { return shift->redis->incr('minion.last_worker_id') }

1;

=encoding utf8

=head1 NAME

Minion::Backend::Redis - Redis backend

=head1 SYNOPSIS

  use Minion::Backend::Redis;

  my $backend = Minion::Backend::Redis->new();

=head1 DESCRIPTION

L<Minion::Backend::Redis> is a backend for L<Minion> based on L<Mojo::Redis>.

=head1 ATTRIBUTES

L<Minion::Backend::Redis> inherits all attributes from L<Minion::Backend> and
implements the following new ones.

=head2 name

  my $name = $backend->name;
  $backend = $backend->name('ns');

Namespace to use with this L<Minion> object. Allows to use multiple L<Minion>
instances with a same Redis server. Defaults to C<ns>.

=head2 redis

  my $redis = $backend->redis;
  $backend  = $backend->redis(Mojo::Redis2->new);

L<Mojo::Redis2> object used to store all data.

=head1 METHODS

L<Minion::Backend::Redis> inherits all methods from L<Minion::Backend> and
implements the following new ones.

=head2 dequeue

  my $job_info = $backend->dequeue($worker_id, 1);
  my $job_info = $backend->dequeue($worker_id, 1, {queues => ['important']});

Wait a given amount of time in seconds for a job, dequeue it and transition from
C<inactive> to C<active> state, or return C<undef> if queues were empty.

These options are currently available:

=over 2

=item queues

  queues => ['important']

One or more queues to dequeue jobs from, defaults to C<default>.

=back

These fields are currently available:

=over 2

=item args

  args => ['foo', 'bar']

Job arguments.

=item id

  id => '10023'

Job ID.

=item retries

  retries => 3

Number of times job has been retried.

=item task

  task => 'foo'

Task name.

=back

=head2 enqueue

  my $job_id = $backend->enqueue('foo');
  my $job_id = $backend->enqueue(foo => [@args]);
  my $job_id = $backend->enqueue(foo => [@args] => {priority => 1});

Enqueue a new job with C<inactive> state.

These options are currently available:

=over 2

=item attempts

  attempts => 25

Number of times performing this job will be attempted, with a delay based on
L<Minion/"backoff"> after the first attempt, defaults to C<1>.

=item delay

  delay => 10

Delay job for this many seconds (from now).

=item parents

  parents => [$id1, $id2, $id3]

One or more existing jobs this job depends on, and that need to have
transitioned to the state C<finished> before it can be processed.

=item priority

  priority => 5

Job priority, defaults to C<0>.

=item queue

  queue => 'important'

Queue to put job in, defaults to C<default>.

=back

=head2 fail_job

  my $bool = $backend->fail_job($job_id, $retries);
  my $bool = $backend->fail_job($job_id, $retries, 'Something went wrong!');
  my $bool = $backend->fail_job(
    $job_id, $retries, {whatever => 'Something went wrong!'});

Transition from C<active> to C<failed> state, and if there are attempts
remaining, transition back to C<inactive> with a delay based on
L<Minion/"backoff">.

=head2 finish_job

  my $bool = $backend->finish_job($job_id, $retries);
  my $bool = $backend->finish_job($job_id, $retries, 'All went well!');
  my $bool = $backend->finish_job(
    $job_id, $retries, {whatever => 'All went well!'});

Transition from C<active> to C<finished> state.

=head2 job_info

  my $job_info = $backend->job_info($job_id);

Get information about a job, or return C<undef> if job does not exist.

  # Check job state
  my $state = $backend->job_info($job_id)->{state};

  # Get job result
  my $result = $backend->job_info($job_id)->{result};

These fields are currently available:

=over 2

=item args

  args => ['foo', 'bar']

Job arguments.

=item attempts

  attempts => 25

Number of times performing this job will be attempted.

=item children

  children => ['10026', '10027', '10028']

Jobs depending on this job.

=item created

  created => 784111777

Epoch time job was created.

=item delayed

  delayed => 784111777

Epoch time job was delayed to.

=item finished

  finished => 784111777

Epoch time job was finished.

=item parents

  parents => ['10023', '10024', '10025']

Jobs this job depends on.

=item priority

  priority => 3

Job priority.

=item queue

  queue => 'important'

Queue name.

=item result

  result => 'All went well!'

Job result.

=item retried

  retried => 784111777

Epoch time job has been retried.

=item retries

  retries => 3

Number of times job has been retried.

=item started

  started => 784111777

Epoch time job was started.

=item state

  state => 'inactive'

Current job state, usually C<active>, C<failed>, C<finished> or C<inactive>.

=item task

  task => 'foo'

Task name.

=item worker

  worker => '154'

Id of worker that is processing the job.

=back

=head2 list_jobs

  my $batch = $backend->list_jobs($offset, $limit);
  my $batch = $backend->list_jobs($offset, $limit, {state => 'inactive'});

Returns the same information as L</"job_info"> but in batches.

These options are currently available:

=over 2

=item queue

  queue => 'important'

List only jobs in this queue.

=item state

  state => 'inactive'

List only jobs in this state.

=item task

  task => 'test'

List only jobs for this task.

=back

=head2 list_workers

  my $batch = $backend->list_workers($offset, $limit);

Returns the same information as L</"worker_info"> but in batches.

=head2 new

  my $backend = Minion::Backend::Redis->new('postgresql://postgres@/test');

Construct a new L<Minion::Backend::Redis> object.

=head2 register_worker

  my $worker_id = $backend->register_worker;
  my $worker_id = $backend->register_worker($worker_id);

Register worker or send heartbeat to show that this worker is still alive.

=head2 remove_job

  my $bool = $backend->remove_job($job_id);

Remove C<failed>, C<finished> or C<inactive> job from queue.

=head2 repair

  $backend->repair;

Repair worker registry and job queue if necessary.

=head2 reset

  $backend->reset;

Reset job queue.

=head2 retry_job

  my $bool = $backend->retry_job($job_id, $retries);
  my $bool = $backend->retry_job($job_id, $retries, {delay => 10});

Transition from C<failed> or C<finished> state back to C<inactive>, already
C<inactive> jobs may also be retried to change options.

These options are currently available:

=over 2

=item delay

  delay => 10

Delay job for this many seconds (from now).

=item priority

  priority => 5

Job priority.

=item queue

  queue => 'important'

Queue to put job in.

=back

=head2 stats

  my $stats = $backend->stats;

Get statistics for jobs and workers.

These fields are currently available:

=over 2

=item active_jobs

  active_jobs => 100

Number of jobs in C<active> state.

=item active_workers

  active_workers => 100

Number of workers that are currently processing a job.

=item delayed_jobs

  delayed_jobs => 100

Number of jobs in C<inactive> state that are scheduled to run at specific time
in the future or have unresolved dependencies. Note that this field is
EXPERIMENTAL and might change without warning!

=item failed_jobs

  failed_jobs => 100

Number of jobs in C<failed> state.

=item finished_jobs

  finished_jobs => 100

Number of jobs in C<finished> state.

=item inactive_jobs

  inactive_jobs => 100

Number of jobs in C<inactive> state.

=item inactive_workers

  inactive_workers => 100

Number of workers that are currently not processing a job.

=back

=head2 unregister_worker

  $backend->unregister_worker($worker_id);

Unregister worker.

=head2 worker_info

  my $worker_info = $backend->worker_info($worker_id);

Get information about a worker, or return C<undef> if worker does not exist.

  # Check worker host
  my $host = $backend->worker_info($worker_id)->{host};

These fields are currently available:

=over 2

=item host

  host => 'localhost'

Worker host.

=item jobs

  jobs => ['10023', '10024', '10025', '10029']

Ids of jobs the worker is currently processing.

=item notified

  notified => 784111777

Epoch time worker sent the last heartbeat.

=item pid

  pid => 12345

Process id of worker.

=item started

  started => 784111777

Epoch time worker was started.

=back

=head1 CREDITS

Most of documentation was copied from L<Minion::Backend::Pg>.

=head1 COPYRIGHT

Andre Parker <andreparker@gmail.com>, 2016.

=head1 LICENSE

This program is free software, you can redistribute it and/or modify it under
the terms of the Artistic License version 2.0.


=cut

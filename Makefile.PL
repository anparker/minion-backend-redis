use strict;
use warnings;

use ExtUtils::MakeMaker;

WriteMakefile(
  NAME         => 'Minion::Backend::Redis',
  VERSION_FROM => 'lib/Minion/Backend/Redis.pm',
  AUTHOR       => 'Andre Parker <andreparker@gmail.com>',
  PREREQ_PM    => {'Mojo::Redis2' => '0.26', 'Minion' => '5.08'},
  test         => {TESTS => 't/*.t'}
);

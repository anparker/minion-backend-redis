# Minion::Backend::Redis

Redis backend for [Minion](https://metacpan.org/pod/Minion) based on 
[Mojo::Redis2](https://metacpan.org/pod/Mojo::Redis).

```perl
use Mojolicious::Lite;

plugin Minion => {Redis => {}};

app->minion->add_task(new_task => sub {...});
app->minion->enqueue('new_tast');

```

Just to be fair, this thing is slower than official Pg backend.

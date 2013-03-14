#!/usr/bin/perl -w

use strict;
use Test::More tests => 15;
use Tie::LevelDB;

my $db = new Tie::LevelDB::DB("/tmp/testdb");

$db->Put("Google", "Don't be evil!");
is($db->Get("Google"), "Don't be evil!", "Google is not evil!");
$db->Delete("Google");
is($db->Get("Google"), undef,'Google is no more');

my $batch = Tie::LevelDB::WriteBatch->new();
$batch->Put("foo1", "bar1");
$batch->Put("foo2", "bar2");
$db->Write($batch);
undef $batch;

is($db->Get("foo1"), "bar1", "foo1 => bar1");
is($db->Get("foo2"), "bar2", "foo2 => bar2");

$db->Delete("foo1");
$db->Delete("foo2");

is($db->Get("foo1"), undef, "foo1 Not Found");
is($db->Get("foo2"), undef, "foo2 Not Found");

my $batch2 = Tie::LevelDB::WriteBatch->new();
$batch2->Put("aaa", "bbb");
$batch2->Put("ccc", "ddd");
$db->Write($batch2);
undef $batch2;

is($db->Get("aaa"), "bbb", "aaa => bbb");
is($db->Get("ccc"), "ddd", "ccc => ddd");

is($db->Get("foo1"), undef, "foo1 Not Found");
is($db->Get("foo2"), undef, "foo2 Not Found");

$batch = Tie::LevelDB::WriteBatch->new();
$batch->Delete("aaa");
$batch->Delete("ccc");
$batch->Put("alice", "bob");
$db->Write($batch);
undef $batch;

is($db->Get("aaa"), undef, "aaa Not Found");
is($db->Get("ccc"), undef, "ccc Not Found");
is($db->Get("alice"), "bob", "alice => bob");
is($db->Get("foo1"), undef, "foo1 Not Found");
is($db->Get("foo2"), undef, "foo2 Not Found");

$db->Delete("alice");
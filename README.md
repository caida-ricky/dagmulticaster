# dagmulticaster

## ndag-telescope configuration

The `ndag-telescope` requires configuration via a YAML configuration file, see `exampleconfig.yaml`. Each sink in the `outputs` list includes packets matching its `filterfile` to a multicast group discoverable via `mcastport` on `mcastaddr`. Exactly one group without a `fitlerfile` is required. This is the default sink for all packets that are neither dropped nor forked.

* *Drop:* Sinks that specify a `filterfile` but no multicast group drop matching packets.

* *Fork:* Each sink has an `exclude` flag which determines whether matching packets are included in the default sink. Per default this flag is to `true` thus forking packets form the default sink. For drop filters this flag has to remain `true`. Other sinks are free to mirror parts of the traffic going to the default sink.

Since filters can overlap and change at runtime conflicts will be resolved as follows:

1. *Drop* filters have the highest priority. Dropped packets will be excluded from all other sinks.
2. *Exclude* filters have priority over mirroring filters. In case of a conflict packets will only go to filters that have the `exclude` option set to `true`.
3. *Mirror* filters duplicate packets that don't conflict with one of the above rules.
4. The *default* sink takes all traffic that is not dropped or excluded.

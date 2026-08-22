create table if not exists session_warm_pool_controllers (
    workload_key text not null,
    controller_id text not null,
    lease_until timestamptz not null,
    updated_at timestamptz not null default now(),
    primary key (workload_key, controller_id)
);

create index if not exists session_warm_pool_controllers_lease_idx
    on session_warm_pool_controllers (workload_key, lease_until);

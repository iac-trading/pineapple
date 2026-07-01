-- =================================================================
-- FACTORY RBAC & PERFORMANCE TUNING V2.0 (96GB RAM OPTIMIZED)
-- =================================================================

-- 1. Gestión de Roles y Usuarios
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{{ vault_postgres_user }}') THEN
        CREATE ROLE {{ vault_postgres_user }} WITH LOGIN PASSWORD '{{ vault_postgres_password }}';
    END IF;
END
$$;

-- 2. Permisos de Esquema
GRANT ALL PRIVILEGES ON DATABASE {{ postgres_db }} TO {{ vault_postgres_user }};
GRANT ALL ON SCHEMA public TO {{ vault_postgres_user }};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {{ vault_postgres_user }};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {{ vault_postgres_user }};

-- 3. Configuración de Conectividad
-- Nota: Asegurar que el archivo pg_hba.conf permita la subred 192.168.100.0/24
ALTER SYSTEM SET max_connections = 500;

-- 4. Optimización de Memoria para 30 Backtests Concurrentes
-- Estos cambios requieren reinicio del contenedor para tener efecto total
ALTER SYSTEM SET shared_buffers = '32GB';
ALTER SYSTEM SET effective_cache_size = '72GB';
ALTER SYSTEM SET maintenance_work_mem = '2GB';
ALTER SYSTEM SET work_mem = '128MB';

-- 5. Paralelismo de TimescaleDB
ALTER SYSTEM SET max_worker_processes = 20;
ALTER SYSTEM SET max_parallel_workers_per_gather = 8;
ALTER SYSTEM SET timescaledb.max_background_workers = 12;

-- 6. Seguridad de solo-lectura para Grafana (Opcional)
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'grafana_read_only') THEN
        CREATE ROLE grafana_read_only WITH LOGIN PASSWORD '{{ vault_grafana_db_password | default("grafana_pass") }}';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE {{ postgres_db }} TO grafana_read_only;
GRANT USAGE ON SCHEMA public TO grafana_read_only;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_read_only;

-- Confirmación de aplicación
SELECT 'Configuración RBAC y Tuning aplicada correctamente' as status;
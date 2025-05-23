-- Function to get all tables with a specific suffix
CREATE OR REPLACE FUNCTION public.get_tables_with_suffix(suffix text)
RETURNS SETOF text AS $$
BEGIN
  RETURN QUERY
  SELECT table_name::text
  FROM information_schema.tables
  WHERE table_schema = 'public'
    AND table_name LIKE '%' || suffix
    AND table_type = 'BASE TABLE';
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to execute SQL statements dynamically (use with caution)
CREATE OR REPLACE FUNCTION public.exec_sql(sql text)
RETURNS void AS $$
BEGIN
  EXECUTE sql;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER; 
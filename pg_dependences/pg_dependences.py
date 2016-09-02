import os
import click
from tabulate import tabulate
import psycopg2
import psycopg2.extras
from graphviz import Digraph

# Output configuration
import logging
format_ = '%(message)s'
logging.basicConfig(format=format_, level=logging.INFO)
logger = logging.getLogger(__name__)


links = list()


def get_linked_objects(conn, schema, table):
    """
    From a database object (table or view), returns all functions and views
     containing/using this object.

    :param conn: psycopg2 connection
    :param schema: schema name to inspect
    :param table: object name to inspect
    :return: psycopg2.extras.DictCursor {type ['VIEW'|'FUNCTION'], schema_name, name}
    """

    sql = """
    WITH f AS (
        SELECT
          'FUNCTION' :: TEXT        AS "type",
          n.nspname                 AS schema_name,
          p.proname                 AS "name",
          regexp_replace(pg_get_functiondef(p.oid), '[\n\r]+', ' ', 'g') AS definition
        FROM pg_catalog.pg_proc p
          INNER JOIN pg_catalog.pg_namespace n ON (n.oid = p.pronamespace)
        WHERE n.nspname NOT IN ('public', 'information_schema', 'pg_catalog') AND p.proname != 'nmul'
        AND NOT (n.nspname = %s AND p.proname = %s)
    ),
    v AS (
        SELECT
          'VIEW'::TEXT AS "type",
          v.schemaname AS schema_name,
          v.viewname AS "name",
          regexp_replace(v.definition, '[\n\r]+', ' ', 'g') AS definition
        FROM pg_catalog.pg_views v
        WHERE v.schemaname NOT IN ('public', 'information_schema', 'pg_catalog', 'nmul')

    ),
    r AS (
        SELECT * FROM f
        UNION SELECT * FROM v
    )

    SELECT type, schema_name, name
    FROM r
    WHERE definition SIMILAR TO %s
    OR definition SIMILAR TO %s
    OR (definition SIMILAR TO %s AND schema_name=%s)
    OR (definition SIMILAR TO %s AND schema_name=%s)
    ORDER BY type, schema_name, name
    """
    key1 = '% (")?{0}(")?.(")?{1}(")? %'.format(schema, table)  # schema_name.table_name surrounded by spaces and optionaly double-quoted
    key2 = '% (")?{0}(")?.(")?{1}(")?\(%'.format(schema, table)  # same, but terminating with a parenthesis, for function
    key3 = '% (")?{0}(")? %'.format(table)  # Just table_name, with optional double-quote (will be limited to the current schema)
    key4 = '% (")?{0}(")?\(%'.format(table)  # Same, but for function
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql, [schema, table, key1, key2, key3, schema, key4, schema])
    return cur.fetchall()


def get_foreign_key(conn, schema, table):
    """
    Returns all tables/views referencing a table

    :param conn: psycopg2 connection
    :param schema: schema name to inspect
    :param table: object name to inspect
    :return: psycopg2.extras.DictCursor {schema_name, table_name, column_name}
    """

    sql = """
    SELECT
      rest.table_schema as schema_name,
      rest.table_name,
      rest.column_name
    FROM (
        SELECT
            a.constraint_catalog, a.constraint_schema, a.constraint_name, a.table_schema, a.table_name,
            array_agg(a.column_name::TEXT) AS column_name
        FROM information_schema.constraint_column_usage a
        GROUP BY a.constraint_catalog, a.constraint_schema, a.constraint_name, a.table_schema, a.table_name
    ) refer
    INNER JOIN information_schema.referential_constraints fkey
        USING (constraint_catalog, constraint_schema, constraint_name)
    INNER JOIN (
        SELECT
            a.constraint_catalog, a.constraint_schema, a.constraint_name, a.table_schema, a.table_name,
            array_agg(a.column_name::TEXT) AS column_name
        FROM (
            SELECT
                *
            FROM information_schema.key_column_usage
            ORDER BY ordinal_position, position_in_unique_constraint
        ) a
        GROUP BY a.constraint_catalog, a.constraint_schema, a.constraint_name, a.table_schema, a.table_name
    ) rest
        USING (constraint_catalog, constraint_schema, constraint_name)
    WHERE refer.table_schema=%s AND refer.table_name=%s
    ORDER BY rest.table_schema, rest.table_name
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql, [schema, table])
    return cur.fetchall()


def get_objects_list(conn, schema):
    """
    List objects names (tables, views) inside a schema

    :param conn: psycopg2 connection
    :param schema: schema name to inspect. Format "schema_name"
    :return: psycopg2.extras.DictCursor {schema_name, table_name}
    """
    sql = """
    SELECT
        table_schema AS schema_name,
        table_name
    FROM information_schema.tables
    WHERE table_schema = %s
    AND table_type IN ('BASE TABLE', 'VIEW')
    ORDER BY table_name ASC
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql, [schema])
    return cur.fetchall()


def graph_linked_objects(g, conn, schema, table):
    """
    Recursively append linked object (view and function) to the Digraph graph
    from the top table object

    :param g: a Digraph object
    :param conn: psycopg2 connection
    :param schema: schema name to inspect
    :param table: object name to inspect
    :return: Nothing
    """

    rows = get_linked_objects(conn, schema, table)
    if len(rows) > 0:
        logger.debug("OBJECT: {0}.{1}".format(schema, table))
    for r in rows:
        node = "{0}.{1}".format(r['schema_name'], r['name'])
        parent_node = "{0}.{1}".format(schema, table)
        if node != parent_node:
            logger.debug("\t- USED IN {0}: {1}".format(r['type'], node))
            if parent_node not in links:  # Used at the first loop for the top level table
                g.attr('node', style='solid', color='black')
                g.node(parent_node)
                links.append(parent_node)
            if r['type'] == 'FUNCTION':
                g.attr('node', style='filled', color='lightblue2')
            else:
                g.attr('node', style='filled', color='lightgrey')
            g.node(node)
            g.edge(parent_node, node)

        if node not in links:
            links.append(node)
            graph_linked_objects(g, conn, r['schema_name'], r['name'])


def graph_foreign_keys(g, conn, schema, table):
    """
    Recursively append linked tables by foreign key constraint to the Digraph graph
    from the top table object
    :param g: a Digraph object
    :param conn: psycopg2 connection
    :param schema: schema name to inspect
    :param table: object name to inspect. Format "schema_name.object_name"
    :return: Nothing
    """

    rows = get_foreign_key(conn, schema, table)
    if len(rows) > 0:
        logger.debug("OBJECT: {0}.{1}".format(schema, table))
    for r in rows:
        node = "{0}.{1}".format(r['schema_name'], r['table_name'])
        logger.debug("\t- REFERENCED BY: {0}".format(node))
        g.attr('node', style='solid', color='black')
        parent_node = "{0}.{1}".format(schema, table)
        g.edge(parent_node, node, label=', '.join(r['column_name']))


@click.command('graph_dependences')
@click.option('-u', '--user', help="Database user name. Default to current user",
              default=lambda: os.environ.get('USER', ''))
@click.option('-P', '--password', prompt=True, hide_input=True, help="User password. WIll be prompted if not set")
@click.option('-h', '--host', help="Database host address. Default to localhost", default='localhost')
@click.option('-d', '--database', help="Database name. Default to current user name",
              default=lambda: os.environ.get('USER', ''))
@click.option('-p', '--port', help="Database port to connect to. Default to 5432", default=5432)
@click.option('-v', '--verbose', help="Verbose mode. Only relevant with --table option", is_flag=True)
@click.option('-t', '--table', help="Generate a detailled cascading graph of all objects related to this table or view")
@click.argument('schema')
def run(user, password, host, database, port, verbose, table, schema):
    """
    Report counts of linked objects and foreign keys at the first level for all tables and views in the specified
    schema.
    With the --table option, generates a pdf graph presenting for this specified top level table (or view) all the
    dependents objects in a cascaded style, i.e. all linked views and functions using these objects, and all tables
    using foreign keys to this top level table, if any.
    """

    if verbose and table:
        logger.setLevel(logging.DEBUG)

    # password = click.prompt("Database password for %s" % user, hide_input=True)

    conn = psycopg2.connect(user=user,
                            password=password,
                            host=host,
                            database=database,
                            port=port)

    if not table:
        res = list()
        for r in get_objects_list(conn, schema):
            ilo = len(get_linked_objects(conn, r['schema_name'], r['table_name']))
            ifk = len(get_foreign_key(conn, r['schema_name'], r['table_name']))
            res.append([r['table_name'], ilo, ifk])
        print(tabulate(res, ["In schema %s" % schema, "first stage links", "foreign keys"]))
    else:
        g = Digraph(name=table, format='pdf')
        g.body.extend(['rankdir=LR', 'size="8,5"'])
        graph_linked_objects(g, conn, schema, table)
        graph_foreign_keys(g, conn, schema, table)
        g.render(cleanup=True)


if __name__ == '__main__':
    run()

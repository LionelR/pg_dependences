import os
import os.path
import click
from tabulate import tabulate
import psycopg2
import psycopg2.extras
import graphviz

# Output configuration
import logging
format_ = '%(message)s'
logging.basicConfig(format=format_, level=logging.INFO)
logger = logging.getLogger(__name__)

STYLES = {
    'BASE TABLE': {'style':'solid', 'color':'black'},
    'FUNCTION': {'style':'filled', 'color':'lightblue2'},
    'VIEW': {'style':'filled', 'color':'lightgrey'}
}

class Table():
    def __init__(self, row):
        """
        An object in the database.
        Can be a table, a view, ...

        :param row: a psycopg2.DictCursor row
        """
        self.schema = row['schema_name']
        self.name = row['table_name']
        self._type = row['type']
    
    def formated(self):
        return "{0}.{1}".format(self.schema, self.name)
    
    def __unicode__(self):
        return "[{0}] {1}.{2}".format(self._type, self.schema, self.name)


class Column():
    def __init__(self, row):
        """
        An column object

        :param row: a psycopg2.DictCursor row
        """
        self.schema = row['schema_name']
        self.table = row['table_name']
        self.name = row['name']
        self._type = 'BASE TABLE'
    
    def formated(self):
        return "{0}.{1}".format(self.schema, self.table)
    
    def fkeys(self):
        return ', '.join(self.name)
    
    def __unicode__(self):
        return "{0}.{1}.{2}".format(self.schema, self.table, self.name)


class Dependences():
    def __init__(self, **kwargs):
        self.conn = psycopg2.connect(**kwargs, connect_timeout=5)
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    def _exec(self, sql, params):
        """
        Execute a query

        :param: sql: SQL string to execute
        :param: params: parameters list to pass to psycopg2
        :return: psycopg2.extras.DictCursor
        """
        self.cur.execute(sql, params)
        return self.cur.fetchall()
    
    def create_table(self, schema, table):
        """
        Create a Table object

        :param schema: schema name
        :param table: object name (table or view)
        :return: a Table object
        """

        sql = """
        SELECT
            table_type AS type,
            table_schema AS schema_name,
            table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        AND table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY 1,2,3
        """

        params = [schema, table]
        return [Table(row) for row in self._exec(sql, params)][0]

    def schema_list(self, schema):
        """
        List tables and views inside a schema

        :param schema: schema name to inspect.
        """
        sql = """
        SELECT
            table_type AS type,
            table_schema AS schema_name,
            table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY 1,2,3
        """
        params = [schema]

        return [Table(row) for row in self._exec(sql, params)]
        
    def childs(self, table):
        """
        Returns all functions and views using the table.

        :return: List of Table objects
        """

        sql = """
        WITH f AS (
            SELECT
            'FUNCTION' :: TEXT AS type,
            n.nspname AS schema_name,
            p.proname AS table_name,
            regexp_replace(pg_get_functiondef(p.oid), E'[\\n\\r]+', ' ', 'g') AS definition
            FROM pg_catalog.pg_proc p
            INNER JOIN pg_catalog.pg_namespace n ON (n.oid = p.pronamespace)
            WHERE n.nspname NOT IN ('public', 'information_schema', 'pg_catalog') AND p.proname != 'nmul'
            AND NOT (n.nspname = %s AND p.proname = %s)
        ),
        v AS (
            SELECT
            'VIEW'::TEXT AS type,
            v.schemaname AS schema_name,
            v.viewname AS table_name,
            regexp_replace(v.definition, E'[\\n\\r]+', ' ', 'g') AS definition
            FROM pg_catalog.pg_views v
            WHERE v.schemaname NOT IN ('public', 'information_schema', 'pg_catalog', 'nmul')

        ),
        r AS (
            SELECT * FROM f
            UNION SELECT * FROM v
        )

        SELECT type, schema_name, table_name
        FROM r
        WHERE definition SIMILAR TO %s
        OR (definition SIMILAR TO %s AND schema_name=%s)
        ORDER BY 1,2,3
        """
        
        params = [
            table.schema,
            table.name,
            '% (\()*(")?{0}(")?.(")?{1}(")?(\))*(;)?(\()? %'.format(table.schema, table.name),
            '% (\()*(")?{0}(")?(\))*(;)?(\()? %'.format(table.name),
            table.schema
        ]

        return [Table(row) for row in self._exec(sql, params)]

    def recursive_childs(self, table):
        """
        Recursively compute all the childs from the top Table
        """

        res = list()
        scanned = [table]
        for parent in scanned:
            childs = self.childs(parent)
            if len(childs) > 0:
                res.append([parent, childs])
                [scanned.append(c) for c in childs if c not in scanned]
                logger.debug("%s Childs= %s" % (parent.__unicode__(), [c.__unicode__() for c in childs]))
        
        return res


    def fkeys(self, table):
        """
        Returns all tables/views referencing a table

        :return: List of Column objects
        """

        sql = """
        SELECT
        rest.table_schema as schema_name,
        rest.table_name,
        rest.column_name AS name
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
        ORDER BY 1,2,3
        """
        
        params = [table.schema, table.name]

        cols = [Column(row) for row in self._exec(sql, params)]
        if len(cols) > 0:
            logger.debug("%s FKeys= %s" % (table.__unicode__(), [c.__unicode__() for c in cols]))
        return table, cols


class Graph():
    def __init__(self, fname, format):
        self.graph =  graphviz.Digraph(fname, format=format)
        self.graph.body.extend(['rankdir=LR', 'size="8,5"'])
        self.plotted = list()
    
    def render(self, childs_list, fkeys_list):
        self.graph_childs(childs_list)
        self.graph_fkeys(fkeys_list)
        path = self.graph.render(cleanup=True)
        return path

    def graph_childs(self, childs_list):
        """
        Add the childs objects to the graph

        :param childs_list: list of [parent, list of childs]
        """

        for parent, childs in childs_list:
            if parent not in self.plotted:
                self.graph.node(parent.formated(), style=STYLES[parent._type]['style'], color=STYLES[parent._type]['color'])
                self.plotted.append(parent)
            for child in childs:
                if child not in self.plotted:
                    self.graph.node(child.formated(), style=STYLES[child._type]['style'], color=STYLES[child._type]['color'])
                    self.plotted.append(child)
                self.graph.edge(parent.formated(), child.formated())

    def graph_fkeys(self, fkeys_list):
        """
        Add the foreign keys objects to the graph

        :param fkeys: list of [parent, list of fkeys]
        """

        parent, childs = fkeys_list
        if parent not in self.plotted:
            self.graph.node(parent.formated(), style=STYLES[parent._type]['style'], color=STYLES[parent._type]['color'])
            self.plotted.append(parent)
        for child in childs:
            if child not in self.plotted:
                self.graph.node(child.formated(), style=STYLES[child._type]['style'], color=STYLES[child._type]['color'])
                self.plotted.append(child)
            self.graph.edge(parent.formated(), child.formated(), label=child.fkeys())


@click.command('pg_dependences')
@click.option('-u', '--user', help="Database user name. Default current user",
              default=lambda: os.environ.get('USER', ''))
@click.option('-P', '--password', prompt=True, hide_input=True, help="User password. Will be prompted if not set")
@click.option('-h', '--host', help="Database host address. Default localhost", default='localhost')
@click.option('-d', '--database', help="Database name. Default current user name",
              default=lambda: os.environ.get('USER', ''))
@click.option('-p', '--port', help="Database port to connect to. Default 5432", default=5432)
@click.option('-v', '--verbose', help="Verbose mode. Only relevant with the --table option and the graph will not be generated", is_flag=True)
@click.option('-t', '--table', help="Generate a detailled cascading graph of all objects related to this table or view")
@click.option('-o', '--output', help="Directory where to put the graph file. Default to home directory and filename formated as schema.table.gv.format")
@click.option('-f', '--format', help="Graph file format (see Graphviz docs for more). Default to pdf", default='pdf')
@click.argument('schema')
def run(user, password, host, database, port, verbose, table, output, format, schema):
    """
    Report counts of linked objects and foreign keys at the first level for all tables and views in the specified
    schema.
    With the --table option, generates a pdf graph presenting for this specified top level table (or view) all the
    dependents objects in a cascaded style, i.e. all linked views and functions using these objects, and all tables
    using foreign keys to this top level table, if any.
    """

    if verbose and table is not None:
        logger.setLevel(logging.DEBUG)

    dep = Dependences(user=user, password=password, host=host, database=database, port=port)

    if not table:
        # Display a listing of all objects dependencies inside the schema
        res = list()
        for table in dep.schema_list(schema):
            ilo = len(dep.childs(table))
            ifk = len(dep.fkeys(table)[1])
            res.append([table.schema, table._type, table.name, ilo, ifk])
        print(tabulate(res, ["Schema", "Type", "Name", "Dependents (first level)", "Foreign keys"]))
    else:
        table = dep.create_table(schema, table)
        childs_list = dep.recursive_childs(table)
        fkeys_list = dep.fkeys(table)
        if not verbose:
            if not output:
                output = os.path.expanduser('~')
            g = Graph(os.path.join(output, table.formated()), format=format)
            path = g.render(childs_list, fkeys_list)
            logger.info("Graph rendered in %s" % path)



if __name__ == '__main__':
    run()

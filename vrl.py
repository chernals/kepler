import os
import getpass
from datetime import datetime
import click
from cassandra.cluster import Cluster
import varilog.converters.matlab

@click.group()
@click.option('--debug/--no-debug', default=False, help='Set the Verbosity of the output')
@click.option('--username', prompt=True, required=True,
              default=lambda: getpass.getuser(), 
              help='Will default to the current username')
@click.pass_context
def cli(ctx, debug, username):
    if debug:
        click.echo('Debug mode is on')
    ctx.obj = {}
    ctx.obj['username'] = username
    pass
    
@cli.command()
def info():
    """Display varilog info.
    
    This will connect to Cassandra and provide summary information.
    """
    cluster = Cluster()
    session = cluster.connect('varilog')
    rows = session.execute("""
    SELECT COUNT(*) FROM md_info
    """)
    for r in rows:
        print(r)
    pass
    
@cli.group()
def md():
    """Operations on MD data.
    """
    pass
    
@md.command()
@click.argument('name', required=True)
@click.option('--comment', prompt=True, required=True)
#@click.confirmation_option(help='Are you sure you want to create a new MD?')
@click.pass_context
def create(ctx, name, comment):
    """Create new MD.
    
    Check if MD name already exists.
    If not, create a new one with a comment and
    add time and user information.
    """
    cluster = Cluster()
    session = cluster.connect('varilog')
    rows = session.execute("""
    SELECT name FROM md_info WHERE name = %s
    """, (name,))
    exists = False
    if len(rows.current_rows) is not 0:
        exists = True
    if not exists:
        stamp = datetime.now()
        session.execute("""
        INSERT INTO md_info(name, md_comment, created, users) VALUES(%s, %s, %s, {%s})
        """, (name, comment, stamp, ctx.obj['username']))
        click.echo("MD created at %s" % stamp)
    else:
        click.echo("This MD already exists!")
    
@md.command()
@click.argument('name', required=True)
@click.argument('tag', required=True)
@click.option('--comment', prompt=True, required=True)
#@click.confirmation_option(help='Are you sure you want to add a new tag?')
@click.pass_context
def tag(ctx, name, tag, comment):
    """Add a tag to an existing MD.
    
    Find the corresponding MD by name.
    If found, check if the tag already exists,
    if not, create a new tag with a comment and
    add time information.
    """
    cluster = Cluster()
    session = cluster.connect('varilog')
    rows = session.execute("""
    SELECT name FROM md_info WHERE name = %s
    """, (name,))
    md_exists = False
    if len(rows.current_rows) is not 0:
        md_exists = True
    if not md_exists:
        click.echo("This MD does not exist, 'create' it first.")
        exit()
        
    rows = session.execute("""
    SELECT name FROM md_info WHERE name = %s and tag = %s
    """, (name, tag))
    tag_exists = False
    if len(rows.current_rows) is not 0:
        tag_exists = True
    if tag_exists:
        click.echo("This tag already exists!")
        exit()
    stamp = datetime.now()
    session.execute("""
    INSERT INTO md_info(name, tag, tag_comment, tag_created, cycle) VALUES(%s, %s, %s, %s, NOW())
    """, (name, tag, comment, stamp))
        
@md.command()
@click.argument('name')
@click.argument('tag')
@click.argument('path')
@click.option('--comment', prompt=True, required=True)
@click.option('--format', default='matlab')
def push(name, tag, path, comment, format):
    if format is not 'matlab':
        click.echo("Only Matlab pulls are supported.")
        exit()
    if format is 'matlab':
        conv = varilog.converters.matlab.Converter(name=name, tag=tag, comment=comment, path=path)
    pass
    
@md.command()
@click.option('--user')
def list(user):
    """Get a complete list of all MD's.
    
    """
    cluster = Cluster()
    session = cluster.connect('varilog')
    rows = session.execute("""
    SELECT DISTINCT name, tag, users FROM md_data
    """)
    tags_by_md = {}
    for r in rows:
        if tags_by_md.get(r[0]) is None:
            tags_by_md[r[0]] = [[r[1], r[2]]]
        else:
            tags_by_md[r[0]].append([r[1], r[2]])
    if user is None:
        print(tags_by_md)
    else:
        print("MD's for %s" % user)
        for md, val in tags_by_md.items():
            for v in val:
                if any(user in s for s in v[1]):
                    print(md ,v[0])

@cli.group()
def ts():
    """Operations on timeseries data.
    
    """
    pass
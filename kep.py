import os
import getpass
from datetime import datetime
import click
from cassandra.cluster import Cluster
import kepler.converters.matlab

@click.group()
@click.option('--debug/--no-debug', default=False, help='Set the Verbosity of the output')
@click.option('--username', required=True,
              default=lambda: getpass.getuser(), 
              help='Will default to the current username')
@click.option('--host', required=True,
              default='cs-ccr-beabp1.cern.ch',
              help='Will default to cs-ccr-beabp1.cern.ch')
@click.pass_context
def cli(ctx, debug, username, host):
    if debug:
        click.echo('Debug mode is on')
    ctx.obj = {}
    ctx.obj['username'] = username
    ctx.obj['host'] = host
    pass
    
@cli.command()
def info():
    """Display Kepler info.
    
    This will connect to Cassandra and provide summary information.
    """
    #cluster = Cluster(['188.184.77.145'])
    cluster = Cluster()
    session = cluster.connect('kepler')
    rows = session.execute("""
    SELECT COUNT(*) FROM md_info
    """)
    for r in rows:
        print("Declared MD's: %d" % r)
    rows = session.execute("""
    SELECT COUNT(*) FROM md_data
    """)
    for r in rows:
        print("MD cycles stored: %d" % r)
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
    session = cluster.connect('kepler')
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
@click.argument('name')
@click.argument('tag')
@click.argument('path')
@click.option('--format', default='matlab')
@click.pass_context
def push(ctx, name, tag, path, format):
    if format is not 'matlab':
        click.echo("Only Matlab pulls are supported.")
        exit()
    if format is 'matlab':
        conv = kepler.converters.matlab.Converter(name=name, tag=tag, path=path, host=ctx.obj['host'])
    
@md.command()
@click.option('--user')
def list(user):
    """Get a complete list of all MD's.
    
    """
    cluster = Cluster()
    session = cluster.connect('kepler')
    rows = session.execute("""
    SELECT name, tag, users FROM md_info
    """)
    for r in rows:
        break
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
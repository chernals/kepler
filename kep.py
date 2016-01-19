import os
import getpass
from datetime import datetime
import click
from cassandra.cluster import Cluster
import kepler.connection
import kepler.converters.matlab

@click.group()
@click.option('--debug/--no-debug', default=False, help='Set the Verbosity of the output')
@click.option('--username', required=True,
              default=getpass.getuser(), 
              help='Will default to the current username')
@click.pass_context
def cli(ctx, debug, username):
    """Kepler CLI entry point.
    
    Set generic options.
    """
    if debug:
        click.echo('Debug mode is on')
    ctx.obj = {}
    ctx.obj['username'] = username
    pass
    
@cli.command()
@click.pass_context
def info(ctx):
    """Display Kepler information.
    
    This will connect to Cassandra and provide summary information.
    """
    rows = kepler.connection._session.execute("""
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
@click.pass_context
def create(ctx, name, comment):
    """Create new MD.
    
    Check if MD name already exists.
    If not, create a new one with a comment and
    add time and user information.
    """
    rows = kepler.connection._session.execute("""
    SELECT name FROM md_info WHERE name = %s
    """, (name,))
    exists = False
    if len(rows.current_rows) is not 0:
        exists = True
    if not exists:
        stamp = datetime.now()
        kepler.connection._session.execute("""
        INSERT INTO md_info(name, md_comment, created, users) VALUES(%s, %s, %s, {%s})
        """, (name, comment, stamp, ctx.obj['username']))
        click.echo("MD created at %s" % stamp)
    else:
        click.echo("This MD already exists!")

@md.command()
@click.argument('name', required=True)
@click.option('--comment', prompt=True, required=True)
@click.option('--user', prompt=True)
@click.pass_context
def update(ctx, name, comment, user):
    """Update information of an existing MD.
    
    Check if MD name already exists.
    If it does, update its associated information.
    """
    kepler.connection._session.execute("""
    INSERT INTO md_info(name, md_comment, users) VALUES (%s, %s, {%s})
    """, (name, comment, user))
    
@md.command()
@click.argument('name', required=True)
@click.argument('user', required=True)
@click.pass_context
def adduser(ctx, name, user):
    """Add user to an existing MD.
    
    Check if MD name already exists.
    If it does, add a user to the user list.
    """
    kepler.connection._session.execute("""
    UPDATE md_info SET users = users + {%s} WHERE name = %s
    """, (user, name))
    
@md.command()
@click.argument('name')
@click.argument('tag')
@click.argument('path')
@click.option('--format', default='matlab')
@click.pass_context
def push(ctx, name, tag, path, format):
    rows = kepler.connection._session.execute("""
    SELECT name FROM md_info WHERE name = %s
    """, (name,))
    exists = False
    if len(rows.current_rows) is not 0:
        exists = True
    if exists is False:
        print("Error: the MD name does not exist!")
        exit()
    
    if format is not 'matlab':
        click.echo("Only Matlab pulls are supported.")
        exit()
    if format is 'matlab':
        conv = kepler.converters.matlab.Converter(name=name, tag=tag, path=path)
    
@md.command()
@click.option('--user')
def list(user):
    """Get a complete list of all MD's.
    
    """
    session = kepler.connection._session
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
    
@cli.command()
@click.option('--file', default='kepler.ics')
@click.pass_context
def ical(ctx, file):
    """Generates iCal file.
    
    """
    cal = kepler.utils.KepCal().save(file)
    
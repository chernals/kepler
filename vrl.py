import os
from datetime import datetime
import click
from cassandra.cluster import Cluster
import varilog.converters.matlab

@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('--username', prompt=True, required=True,
              default=lambda: os.environ.get('VARILOG_USERNAME', ''))
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
    SELECT COUNT(*) FROM md_data
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
@click.argument('tag', required=True)
@click.option('--comment', prompt=True, required=True)
@click.confirmation_option(help='Are you sure you want to create a new MD?')
@click.pass_context
def create(ctx, name, tag, comment):
    """Create new MD.
    
    """
    cluster = Cluster()
    session = cluster.connect('varilog')
    rows = session.execute("""
    SELECT DISTINCT name, tag FROM md_data
    """)
    exists = False
    for r in rows:
        if r[0] == name:
            exists = True
    if not exists:
        stamp = datetime.now()
        session.execute("""
        INSERT INTO md_data(name, tag, comment, created_at, users) VALUES(%s, %s, %s, %s, {%s})
        """, (name, tag, comment, stamp, ctx.obj['username']))
        click.echo("MD created at %s" % stamp)
    else:
        click.echo("This MD already exists!")
    
@md.command()
@click.argument('name', required=True)
@click.argument('tag', required=True)
@click.option('--comment', prompt=True, required=True)
@click.confirmation_option(help='Are you sure you want to add a new tag?')
@click.pass_context
def tag(ctx, name, tag, comment):
    """Add a tag to an existing MD.
    
    """
    cluster = Cluster()
    session = cluster.connect('varilog')
    rows = session.execute("""
    SELECT DISTINCT name, tag FROM md_data
    """)
    md_exists = False
    tag_exists = False
    users = None
    for r in rows:
        if r[0] == name:
            md_exists = True
        if r[0] == name and r[1] == tag:
            tag_exists = True
    if md_exists and not tag_exists:
        stamp = datetime.now()
        session.execute("""
        INSERT INTO md_data(name, tag, comment, created_at, users) VALUES(%s, %s, %s, %s, {%s})
        """, (name, tag, comment, stamp, ctx.obj['username']))
    elif md_exists and tag_exists:
        click.echo("This tag already exists!")
    else:
        click.echo("This MD doesn't exist, 'create' it first.")
        
@md.command()
@click.argument('name')
@click.argument('tag')
@click.argument('path')
@click.option('--comment', prompt=True, required=True)
@click.option('--format', default='matlab')
def pull(name, tag, path, comment, format):
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
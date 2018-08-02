import click
import time

def hello(count, name):
    """Simple program that greets NAME for a total of COUNT times."""
    for x in range(count):
        click.echo('Hello %s!' % name)

@click.command()
@click.option('--count', default=1, help='Number of greetings.')
@click.option('--name', default="Danny",
              help='The person to greet.')
def main(count, name):
    for i in range(4):
        hello(count, name)
        print(i)
        time.sleep(4)


if __name__ == '__main__':
    main()

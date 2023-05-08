"""
Tasks
"""

from invoke import task


@task()
def lint(ctx):
    print('Analying package: main')
    ctx.run('pylint -rn main')
    print('Analysing package: tests')
    ctx.run('pylint -rn tests')
    print('Analysing package: utils')
    ctx.run('pylint -rn utils')


@task(pre=[lint])
def unit_test(ctx):
    print('Starting unit-tests')
    cmd = 'PYTHONPATH=. pytest ./tests/unit'
    ctx.run(cmd, hide=False, warn=True)

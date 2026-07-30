import os
import sys
import subprocess


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else "all"

    # Define test directories and corresponding coverage targets
    test_config = {
        'unit': {'dir': 'tests/unit_tests', 'cov': ['config', 'src']},
        'integration': {'dir': 'tests/integration_tests', 'cov': []},
        'component': {'dir': 'tests/component_tests', 'cov': []},
        'all': {'dir': 'tests', 'cov': ['config', 'src']},
    }

    env = os.environ.copy()
    env['ENV'] = 'test'

    if command in test_config:
        test_dir = test_config[command]['dir']
        cov_sources = ','.join(test_config[command]['cov'])

        if cov_sources:
            cmd = [
                sys.executable,
                "-m",
                "coverage",
                "run",
                f"--source={cov_sources}",
                "--omit=*/__init__.py",
                "-m",
                "pytest",
                "--verbose",
                test_dir,
            ]
            subprocess.run(cmd, env=env, check=True)
            subprocess.run([sys.executable, "-m", "coverage", "report", "-m"], env=env)
        else:
            cmd = [sys.executable, "-m", "pytest", "--verbose", test_dir]
            subprocess.run(cmd, env=env, check=True)
    elif command == 'lint':
        subprocess.run([sys.executable, "-m", "flake8", "."], env=env)
    else:
        raise ValueError(f"Unknown command: {command}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError(
            "Usage: run_tests.py <unit|integration|component|all|lint>"
        )
    else:
        main()

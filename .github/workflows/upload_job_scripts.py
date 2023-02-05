import os

def main():
  print("Hello from GitHub Actions!")
  token = os.environ.get("AWS_ACCESS_ID")
  if not token:
    raise RuntimeError("AWS_ACCESS_ID env var is not set!")
  print("All good! we found our env var")
  

if __name__ == '__main__':
  main()
def loop(n):
    while (n - 1) >= 0:
        print(f"{(n - 1) * (n - 1)}")
        n = n - 1


if __name__ == "__main__":
    n = int(input())
    loop(n)

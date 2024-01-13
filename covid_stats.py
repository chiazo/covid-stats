from cities.city import City


def main():
    result = City("new_york").get_df().head(2)

    print(result)


if __name__ == "__main__":
    main()

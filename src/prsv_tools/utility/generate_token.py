import prsv_tools.utility.api as prsvapi
import prsv_tools.utility.cli as prsvcli


def main():
    parser = prsvcli.Parser()
    parser.add_instance()

    args = parser.parse_args()

    token = prsvapi.get_token(f"{args.instance}-manage")

    print(token)


if __name__ == "__main__":
    main()

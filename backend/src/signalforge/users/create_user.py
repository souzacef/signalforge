from argparse import ArgumentParser, Namespace
from asyncio import run
from getpass import getpass
from sys import stderr

from pydantic import SecretStr, ValidationError
from sqlalchemy.exc import IntegrityError

from signalforge.auth.service import create_user, get_user_by_email
from signalforge.db.session import async_session_factory
from signalforge.users.models import UserRole
from signalforge.users.schemas import UserCreate


def _parse_args() -> Namespace:
    parser = ArgumentParser(description="Create a SignalForge user")
    parser.add_argument("--email", required=True)
    parser.add_argument(
        "--role",
        required=True,
        choices=[role.value for role in UserRole],
    )
    return parser.parse_args()


async def _create_user(args: Namespace) -> int:
    password = getpass("Password: ")
    confirmation = getpass("Confirm password: ")
    if password != confirmation:
        print("Passwords do not match.", file=stderr)
        return 1

    try:
        user_data = UserCreate(
            email=args.email,
            password=SecretStr(password),
            role=args.role,
        )
    except ValidationError:
        print(
            "Invalid email or password; passwords must contain 12 to 128 characters.",
            file=stderr,
        )
        return 1

    async with async_session_factory() as session:
        if await get_user_by_email(session, user_data.email) is not None:
            print("A user with that email already exists.", file=stderr)
            return 1

        try:
            user = await create_user(session, user_data)
        except IntegrityError:
            await session.rollback()
            print("A user with that email already exists.", file=stderr)
            return 1

    print(f"Created {user.role.value} user {user.email}.")
    return 0


def main() -> int:
    return run(_create_user(_parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())

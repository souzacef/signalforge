from signalforge.auth.passwords import hash_password, verify_password


def test_password_hashing_and_verification() -> None:
    password = "correct horse battery staple"

    password_hash = hash_password(password)

    assert password_hash != password
    assert verify_password(password, password_hash)
    assert not verify_password("incorrect password", password_hash)

import pytest

from amongo.core.models import Flags


def test_verify_method_valid_flags():
    # Test valid flags without exhaust_allowed
    valid_flags = Flags.checksum_present | Flags.more_to_come
    assert valid_flags.verify() == valid_flags

    # Test valid flags with exhaust_allowed
    valid_flags = Flags.checksum_present | Flags.more_to_come | Flags.exhaust_allowed
    assert valid_flags.verify() == valid_flags


def test_verify_method_invalid_flags():
    # Test invalid flags with unknown bit set
    with pytest.raises(ValueError, match="Unknown bit set in flags"):
        invalid_flags = Flags(1 << 20)  # Setting an unknown bit
        invalid_flags.verify()

    # Test invalid flags with some unknown bits set
    with pytest.raises(ValueError, match="Unknown bit set in flags"):
        invalid_flags = Flags.checksum_present | Flags.more_to_come | (1 << 20)
        invalid_flags.verify()


def test_verify_method_return_type():
    # Verify that the verify method returns the correct type
    flags = Flags.checksum_present
    assert isinstance(flags.verify(), Flags)


def test_verify_method_with_all_flag():
    # Test the `all` flag combination with the verify method
    all_flags = Flags.all
    assert all_flags.verify() == all_flags


def test_verify_method_with_no_flags():
    # Test an instance of Flags with no flags set
    no_flags = Flags(0)
    assert no_flags.verify() == no_flags

from Create_Table import *
from unittest import Mock


def test_get_env_variables():
    mock_os_library = Mock()
    mock_os_library.environ.get.return_value = "test"

    result = get_env_variables(mock_os_library)

    assert result == {"host": "test", "user": "test",
                      "password": "test", "database": "test"}


def test_connect_to_db():
    mock_postgres_library = Mock()
    test_env_variables = {"host": "test", "user": "test",
                          "password": "test", "database": "test"}

    result = connect_to_db(test_get_env_variables, mock_postgres_library)

    assert result


def test_create_orders_table():
    mock_connect_to_db_function = Mock()
    mock_get_cursor = Mock()

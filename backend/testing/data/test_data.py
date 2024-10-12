"Test Suite for 'dat' package"

import sys
import unittest
from unittest.mock import Mock, MagicMock, patch, call


class Test_100_Data_Package(unittest.TestCase):
    def setUp(self):
        # Remove the package/module from sys.modules if it is already imported
        for mod in ["data"]:
            if mod in sys.modules:
                print(f"{sys.modules[mod]=}")
                del sys.modules[mod]

    def test_100_import_business_objects(self):
        MOCK_BASE_PARENT = "mock_base_parent"
        MOCK_PKG1_MOD1 = "mock_pkg1.mock_module1"
        MOCK_PKG2_MOD2 = "mock_pkg2.mock_module2"
        with (
            patch("pathlib.Path") as Mock_Path,
            patch("importlib.import_module") as mock_import_module,
        ):
            # Mock the pathlib methods to simulate the package directory
            mock_path = Mock()
            mock_base_path = Mock()
            mock_base_path.parent = MOCK_BASE_PARENT
            mock_rel1 = Mock()
            mock_rel1.name = "__mock__.py"
            mock_rel2 = Mock()
            mock_rel2.name = MOCK_PKG1_MOD1.split(".")[1] + ".py"
            mock_parts2 = Mock()
            mock_parts2.parts = MOCK_PKG1_MOD1.split(".")
            mock_rel2.with_suffix = Mock(return_value=mock_parts2)
            mock_rel3 = Mock()
            mock_rel3.name = MOCK_PKG2_MOD2.split(".")[1] + ".py"
            mock_parts3 = Mock()
            mock_parts3.parts = MOCK_PKG2_MOD2.split(".")
            mock_rel3.with_suffix = Mock(return_value=mock_parts3)
            mock_abs_path = Mock()
            mock_abs_path.relative_to = Mock(
                side_effect=[mock_rel1, mock_rel2, mock_rel3],
            )
            mock_base_path.rglob.return_value = iter(
                [mock_abs_path, mock_abs_path, mock_abs_path]
            )
            mock_path.parent = mock_base_path
            Mock_Path.return_value = mock_path

            mock_reg_cls_1 = Mock(name="mock_register_persistant_class")
            mock_class1 = type(
                "MockClass1",
                (object,),
                {
                    "__module__": MOCK_PKG1_MOD1,
                    "register_persistant_class": mock_reg_cls_1,
                },
            )
            mock_reg_cls_2 = Mock(name="mock_register_persistant_class")
            mock_class2 = type(
                "MockClass2",
                (object,),
                {
                    "__module__": MOCK_PKG2_MOD2,
                    "no_register_persistant_class": mock_reg_cls_2,
                },
            )
            mock_module1 = Mock(name="mock_module1")
            mock_module1.MockClass1 = mock_class1
            mock_module1.__dict__["MockClass1"] = mock_class1
            mock_module2 = Mock(name="mock_module2")
            mock_module2.MockClass2 = mock_class2
            mock_module2.__dict__["MockClass2"] = mock_class2
            mock_import_module.side_effect = [mock_module1, mock_module2]
            mock_import_module.reset_mock()

            import data

            Mock_Path.assert_called_once()
            self.assertRegex(Mock_Path.call_args.args[0], r"\\data\\__init__\.py$")
            mock_base_path.rglob.assert_called_with("*.py")
            self.assertEqual(
                mock_abs_path.relative_to.call_count,
                3,
                "Path.relative_to() should be called 3 times",
            )
            mock_abs_path.relative_to.assert_called_with(MOCK_BASE_PARENT)
            print(f"{mock_import_module.call_args_list=}")
            self.assertEqual(
                mock_import_module.call_count,
                2,
                "importlib.import_module() should be called twice",
            )
            self.assertListEqual(
                mock_import_module.call_args_list,
                [call(name=MOCK_PKG1_MOD1), call(name=MOCK_PKG2_MOD2)],
            )
            mock_reg_cls_1.assert_called_once_with()
            mock_reg_cls_2.assert_not_called()


if __name__ == "__main__":
    unittest.main()

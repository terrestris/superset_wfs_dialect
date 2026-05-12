import pytest
from unittest.mock import MagicMock, Mock, patch

from superset_wfs_dialect.custom_postrequest import PostRequest_2_0_0


class TestCustomPostRequest:
    """Tests for the custom PostRequest_2_0_0 class."""

    @pytest.fixture
    def mock_post_request(self):
        """Create a mock PostRequest instance with a mock root element."""
        # Create a real instance but we'll mock the _root
        request = Mock(spec=PostRequest_2_0_0)

        # Mock the root element
        mock_root = MagicMock()
        mock_root.tag = "wfs:GetFeature"
        mock_root.nsmap = {}
        mock_root.attrib = {"version": "2.0.0", "service": "WFS"}

        # Mock children
        child1 = MagicMock()
        child2 = MagicMock()
        mock_root.__getitem__ = MagicMock(return_value=[child1, child2])

        request._root = mock_root

        return request

    @patch('superset_wfs_dialect.custom_postrequest.etree.Element')
    def test_register_single_typename_with_namespace(self, mock_element_class, mock_post_request):
        """Test registering a single typename with namespace prefix."""
        # Setup
        wfs_nsmap = {
            "test": "http://example.com/test",
            "other": "http://example.com/other"
        }

        mock_new_root = MagicMock()
        mock_new_root.tag = "wfs:GetFeature"
        mock_element_class.return_value = mock_new_root

        # Call the method
        PostRequest_2_0_0.register_typenames_ns(
            mock_post_request,
            typenames="test:layer",
            wfs_nsmap=wfs_nsmap
        )

        # Verify Element was called with updated nsmap
        called_nsmap = mock_element_class.call_args[1]['nsmap']
        assert "test" in called_nsmap
        assert called_nsmap["test"] == "http://example.com/test"

        # Verify the new root was assigned
        assert mock_post_request._root == mock_new_root

    @patch('superset_wfs_dialect.custom_postrequest.etree.Element')
    def test_register_multiple_typenames_with_namespace(self, mock_element_class, mock_post_request):
        """Test registering multiple typenames with namespace prefixes."""
        # Setup
        wfs_nsmap = {
            "test": "http://example.com/test",
            "other": "http://example.com/other",
            "geo": "http://example.com/geo"
        }

        mock_new_root = MagicMock()
        mock_element_class.return_value = mock_new_root

        # Call the method with multiple typenames
        PostRequest_2_0_0.register_typenames_ns(
            mock_post_request,
            typenames="test:layer1,other:layer2,geo:layer3",
            wfs_nsmap=wfs_nsmap
        )

        # Verify Element was called with all namespaces
        called_nsmap = mock_element_class.call_args[1]['nsmap']
        assert "test" in called_nsmap
        assert called_nsmap["test"] == "http://example.com/test"
        assert "other" in called_nsmap
        assert called_nsmap["other"] == "http://example.com/other"
        assert "geo" in called_nsmap
        assert called_nsmap["geo"] == "http://example.com/geo"

    @patch('superset_wfs_dialect.custom_postrequest.etree.Element')
    def test_register_typename_without_colon_ignored(self, mock_element_class, mock_post_request):
        """Test that typenames without colons are ignored."""
        # Setup
        wfs_nsmap = {
            "test": "http://example.com/test"
        }

        mock_new_root = MagicMock()
        mock_element_class.return_value = mock_new_root

        # Call the method with typename without namespace prefix
        PostRequest_2_0_0.register_typenames_ns(
            mock_post_request,
            typenames="simplelayer",
            wfs_nsmap=wfs_nsmap
        )

        # Verify Element was called with empty nsmap (no namespaces added)
        called_nsmap = mock_element_class.call_args[1]['nsmap']
        assert "test" not in called_nsmap

    @patch('superset_wfs_dialect.custom_postrequest.etree.Element')
    def test_register_prefix_not_in_wfs_nsmap_ignored(self, mock_element_class, mock_post_request):
        """Test that prefixes not in wfs_nsmap are ignored."""
        # Setup
        wfs_nsmap = {
            "test": "http://example.com/test"
        }

        mock_new_root = MagicMock()
        mock_element_class.return_value = mock_new_root

        # Call the method with unknown prefix
        PostRequest_2_0_0.register_typenames_ns(
            mock_post_request,
            typenames="unknown:layer",
            wfs_nsmap=wfs_nsmap
        )

        # Verify Element was called but unknown prefix not added
        called_nsmap = mock_element_class.call_args[1]['nsmap']
        assert "unknown" not in called_nsmap

    @patch('superset_wfs_dialect.custom_postrequest.etree.Element')
    def test_new_root_has_correct_tag(self, mock_element_class, mock_post_request):
        """Test that new root element is created with correct tag."""
        # Setup
        wfs_nsmap = {"test": "http://example.com/test"}

        mock_new_root = MagicMock()
        mock_element_class.return_value = mock_new_root

        # Call the method
        PostRequest_2_0_0.register_typenames_ns(
            mock_post_request,
            typenames="test:layer",
            wfs_nsmap=wfs_nsmap
        )

        # Verify Element was called with original tag
        assert mock_element_class.call_args[0][0] == "wfs:GetFeature"

    @patch('superset_wfs_dialect.custom_postrequest.etree.Element')
    def test_children_copied_to_new_root(self, mock_element_class, mock_post_request):
        """Test that children are copied to the new root element."""
        # Setup
        wfs_nsmap = {"test": "http://example.com/test"}

        mock_new_root = MagicMock()
        mock_element_class.return_value = mock_new_root

        # Call the method
        PostRequest_2_0_0.register_typenames_ns(
            mock_post_request,
            typenames="test:layer",
            wfs_nsmap=wfs_nsmap
        )

        # Verify children were copied (via __setitem__)
        assert mock_new_root.__setitem__.called

    @patch('superset_wfs_dialect.custom_postrequest.etree.Element')
    def test_attributes_copied_to_new_root(self, mock_element_class, mock_post_request):
        """Test that attributes are copied to the new root element."""
        # Setup
        wfs_nsmap = {"test": "http://example.com/test"}

        mock_new_root = MagicMock()
        mock_new_root.attrib = MagicMock()
        mock_element_class.return_value = mock_new_root

        # Call the method
        PostRequest_2_0_0.register_typenames_ns(
            mock_post_request,
            typenames="test:layer",
            wfs_nsmap=wfs_nsmap
        )

        # Verify attributes were updated
        mock_new_root.attrib.update.assert_called_once_with(
            {"version": "2.0.0", "service": "WFS"}
        )

    @patch('superset_wfs_dialect.custom_postrequest.etree.Element')
    def test_existing_nsmap_preserved(self, mock_element_class, mock_post_request):
        """Test that existing namespace mappings are preserved."""
        # Setup - mock_post_request already has empty nsmap, let's add to it
        mock_post_request._root.nsmap = {"wfs": "http://www.opengis.net/wfs/2.0"}

        wfs_nsmap = {
            "test": "http://example.com/test"
        }

        mock_new_root = MagicMock()
        mock_element_class.return_value = mock_new_root

        # Call the method
        PostRequest_2_0_0.register_typenames_ns(
            mock_post_request,
            typenames="test:layer",
            wfs_nsmap=wfs_nsmap
        )

        # Verify Element was called with both existing and new namespaces
        called_nsmap = mock_element_class.call_args[1]['nsmap']
        assert "wfs" in called_nsmap
        assert called_nsmap["wfs"] == "http://www.opengis.net/wfs/2.0"
        assert "test" in called_nsmap
        assert called_nsmap["test"] == "http://example.com/test"

    @patch('superset_wfs_dialect.custom_postrequest.etree.Element')
    def test_duplicate_prefixes_use_last_mapping(self, mock_element_class, mock_post_request):
        """Test that duplicate prefixes in typenames use the same mapping."""
        # Setup
        wfs_nsmap = {
            "test": "http://example.com/test"
        }

        mock_new_root = MagicMock()
        mock_element_class.return_value = mock_new_root

        # Call with duplicate prefix in different typenames
        PostRequest_2_0_0.register_typenames_ns(
            mock_post_request,
            typenames="test:layer1,test:layer2,test:layer3",
            wfs_nsmap=wfs_nsmap
        )

        # Verify prefix added only once (same mapping)
        called_nsmap = mock_element_class.call_args[1]['nsmap']
        assert "test" in called_nsmap
        assert called_nsmap["test"] == "http://example.com/test"

    @patch('superset_wfs_dialect.custom_postrequest.etree.Element')
    def test_empty_typenames_string(self, mock_element_class, mock_post_request):
        """Test handling of empty typenames string."""
        # Setup
        wfs_nsmap = {"test": "http://example.com/test"}

        mock_new_root = MagicMock()
        mock_element_class.return_value = mock_new_root

        # Call with empty string
        PostRequest_2_0_0.register_typenames_ns(
            mock_post_request,
            typenames="",
            wfs_nsmap=wfs_nsmap
        )

        # Verify Element was still called (though no namespaces added)
        assert mock_element_class.called
        assert mock_post_request._root == mock_new_root

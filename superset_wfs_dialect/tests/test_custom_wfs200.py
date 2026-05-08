import pytest
from unittest.mock import MagicMock, Mock, patch
from urllib.parse import parse_qs, urlparse

from superset_wfs_dialect.custom_wfs200 import WebFeatureService_2_0_0


class TestCustomWFS200:
    """Tests for the custom WebFeatureService_2_0_0 class."""

    @pytest.fixture
    def mock_wfs_instance(self):
        """Create a mock WFS instance with basic configuration."""
        # Create a mock instance without calling __init__
        wfs = Mock(spec=WebFeatureService_2_0_0)
        wfs.version = "2.0.0"
        wfs.url = "https://example.com/wfs"
        wfs.timeout = 30
        wfs.headers = {}
        wfs.auth = None

        # Mock the getOperationByName method
        mock_operation = MagicMock()
        mock_operation.methods = [{"type": "Get", "url": "https://example.com/wfs"}]
        wfs.getOperationByName.return_value = mock_operation

        # Mock getSRS method
        wfs.getSRS.return_value = "EPSG:4326"

        return wfs

    @pytest.fixture
    def mock_wfs_instance_post(self):
        """Create a mock WFS instance configured for POST requests."""
        wfs = Mock(spec=WebFeatureService_2_0_0)
        wfs.version = "2.0.0"
        wfs.url = "https://example.com/wfs"
        wfs.timeout = 30
        wfs.headers = {}
        wfs.auth = None

        # Mock the getOperationByName method for POST
        mock_operation = MagicMock()
        mock_operation.methods = [{"type": "Post", "url": "https://example.com/wfs"}]
        wfs.getOperationByName.return_value = mock_operation

        # Mock identification for namespace mapping
        mock_root = MagicMock()
        mock_root.nsmap = {"test": "http://example.com/test"}
        wfs.identification = MagicMock()
        wfs.identification._root = mock_root

        # Mock getBBOXPost method
        wfs.getBBOXPost.return_value = "bbox_element"

        return wfs

    @patch('superset_wfs_dialect.custom_wfs200.openURL')
    def test_resulttype_parameter_in_get_request(self, mock_openurl, mock_wfs_instance):
        """Test that resultType parameter is included in GET requests."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.info.return_value = {"Content-Length": "100"}
        mock_response.read.return_value = b'<FeatureCollection></FeatureCollection>'
        mock_openurl.return_value = mock_response

        # Call the actual method
        url = WebFeatureService_2_0_0.getGETGetFeatureRequest(
            mock_wfs_instance,
            result_type="hits",
            typename="test:layer"
        )

        # Parse the URL and check for resultType parameter
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        assert "resultType" in query_params
        assert query_params["resultType"][0] == "hits"

    @patch('superset_wfs_dialect.custom_wfs200.openURL')
    def test_filter_parameter_name_in_get_request(self, mock_openurl, mock_wfs_instance):
        """Test that 'filter' parameter is used (not 'query') in GET requests."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.info.return_value = {"Content-Length": "100"}
        mock_response.read.return_value = b'<FeatureCollection></FeatureCollection>'
        mock_openurl.return_value = mock_response

        # Call the actual method with filter parameter
        url = WebFeatureService_2_0_0.getGETGetFeatureRequest(
            mock_wfs_instance,
            typename="test:layer",
            filter="<Filter>test filter</Filter>"
        )

        # Parse the URL and check for filter parameter
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        assert "filter" in query_params
        assert query_params["filter"][0] == "<Filter>test filter</Filter>"
        # Ensure 'query' is not used
        assert "query" not in query_params

    @patch('superset_wfs_dialect.custom_wfs200.openURL')
    def test_srsname_parameter_in_get_request(self, mock_openurl, mock_wfs_instance):
        """Test that srsName parameter is supported in GET requests."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.info.return_value = {"Content-Length": "100"}
        mock_response.read.return_value = b'<FeatureCollection></FeatureCollection>'
        mock_openurl.return_value = mock_response

        # Call the actual method with srsname parameter
        url = WebFeatureService_2_0_0.getGETGetFeatureRequest(
            mock_wfs_instance,
            typename="test:layer",
            srsname="EPSG:3857"
        )

        # Parse the URL and check for srsname parameter
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        assert "srsname" in query_params
        assert query_params["srsname"][0] == "EPSG:3857"

        # Verify that getSRS was called to validate the SRS
        mock_wfs_instance.getSRS.assert_called_once_with("EPSG:3857", "test:layer")

    @patch('superset_wfs_dialect.custom_wfs200.PostRequest_2_0_0')
    def test_outputformat_query_parameter_in_post_request(self, mock_post_request_class, mock_wfs_instance_post):
        """Test that outputFormat is added as query parameter in POST requests."""
        # Setup mock PostRequest
        mock_request = MagicMock()
        mock_request.to_string.return_value = "<GetFeature>...</GetFeature>"
        mock_post_request_class.return_value = mock_request

        # Mock the create_post_request method to return our mock
        mock_wfs_instance_post.create_post_request.return_value = mock_request

        # Call the actual method with outputFormat
        url, data = WebFeatureService_2_0_0.getPOSTGetFeatureRequest(
            mock_wfs_instance_post,
            typename="test:layer",
            outputFormat="application/json"
        )

        # Verify that outputFormat is set on the request object
        mock_request.set_outputformat.assert_called_once_with("application/json")

        # Parse the URL and verify outputFormat is in query parameters
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        assert "outputFormat" in query_params
        assert query_params["outputFormat"][0] == "application/json"

    @patch('superset_wfs_dialect.custom_wfs200.openURL')
    def test_resulttype_parameter_in_get_request_with_results(self, mock_openurl, mock_wfs_instance):
        """Test that resultType='results' works in GET requests."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.info.return_value = {"Content-Length": "100"}
        mock_response.read.return_value = b'<FeatureCollection></FeatureCollection>'
        mock_openurl.return_value = mock_response

        # Call the actual method
        url = WebFeatureService_2_0_0.getGETGetFeatureRequest(
            mock_wfs_instance,
            result_type="results",
            typename="test:layer"
        )

        # Parse the URL and check for resultType parameter
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        assert "resultType" in query_params
        assert query_params["resultType"][0] == "results"

    @patch('superset_wfs_dialect.custom_wfs200.PostRequest_2_0_0')
    def test_filter_parameter_in_post_request(self, mock_post_request_class, mock_wfs_instance_post):
        """Test that filter parameter is used in POST requests."""
        # Setup mock PostRequest
        mock_request = MagicMock()
        mock_request.to_string.return_value = "<GetFeature>...</GetFeature>"
        mock_post_request_class.return_value = mock_request

        # Mock the create_post_request method to return our mock
        mock_wfs_instance_post.create_post_request.return_value = mock_request

        # Call the actual method with filter
        WebFeatureService_2_0_0.getPOSTGetFeatureRequest(
            mock_wfs_instance_post,
            typename="test:layer",
            filter="<Filter>test filter</Filter>"
        )

        # Verify that set_filter was called with the filter parameter
        mock_request.set_filter.assert_called_once_with("<Filter>test filter</Filter>")

    @patch('superset_wfs_dialect.custom_wfs200.PostRequest_2_0_0')
    def test_srsname_parameter_in_post_request(self, mock_post_request_class, mock_wfs_instance_post):
        """Test that srsName parameter is supported in POST requests."""
        # Setup mock PostRequest
        mock_request = MagicMock()
        mock_request.to_string.return_value = "<GetFeature>...</GetFeature>"
        mock_post_request_class.return_value = mock_request

        # Mock the create_post_request method to return our mock
        mock_wfs_instance_post.create_post_request.return_value = mock_request

        # Call the actual method with srsname
        WebFeatureService_2_0_0.getPOSTGetFeatureRequest(
            mock_wfs_instance_post,
            typename="test:layer",
            srsname="EPSG:3857"
        )

        # Verify that set_srsname was called with the srsname parameter
        mock_request.set_srsname.assert_called_once_with("EPSG:3857")

    @patch('superset_wfs_dialect.custom_wfs200.PostRequest_2_0_0')
    def test_resulttype_parameter_in_post_request(self, mock_post_request_class, mock_wfs_instance_post):
        """Test that resultType parameter is supported in POST requests."""
        # Setup mock PostRequest
        mock_request = MagicMock()
        mock_request.to_string.return_value = "<GetFeature>...</GetFeature>"
        mock_post_request_class.return_value = mock_request

        # Mock the create_post_request method to return our mock
        mock_wfs_instance_post.create_post_request.return_value = mock_request

        # Call the actual method with result_type
        WebFeatureService_2_0_0.getPOSTGetFeatureRequest(
            mock_wfs_instance_post,
            typename="test:layer",
            result_type="hits"
        )

        # Verify that set_resulttype was called with the result_type parameter
        mock_request.set_resulttype.assert_called_once_with("hits")

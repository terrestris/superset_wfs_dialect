import requests

from owslib.etree import etree, ParseError
from owslib.util import Authentication, ServiceException, ResponseWrapper

### Custom patch for owslib.util.openURL to set the Content-Type header for WFS POST to
### text/xml; charset=utf-8. As soon as this is fixed in owslib, this file can be removed and the
### original openURL can be used instead.
def openURL(url_base, data=None, method='Get', cookies=None, username=None, password=None, timeout=30, headers=None,
            verify=True, cert=None, auth=None):
    """
    Function to open URLs.

    Uses requests library but with additional checks for OGC service exceptions and url formatting.
    Also handles cookies and simple user password authentication.

    :param headers: (optional) Dictionary of HTTP Headers to send with the :class:`Request`.
    :param verify: (optional) whether the SSL cert will be verified. A CA_BUNDLE path can also be provided.
                   Defaults to ``True``.
    :param cert: (optional) A file with a client side certificate for SSL authentication
                 to send with the :class:`Request`.
    :param auth: Instance of owslib.util.Authentication
    """

    headers = headers if headers is not None else {}
    rkwargs = {}

    rkwargs['timeout'] = timeout

    if auth:
        if username:
            auth.username = username
        if password:
            auth.password = password
        if cert:
            auth.cert = cert
        verify = verify and auth.verify
    else:
        auth = Authentication(username, password, cert, verify)

    if auth.username and auth.password:
        rkwargs['auth'] = (auth.username, auth.password)
    elif auth.auth_delegate is not None:
        rkwargs['auth'] = auth.auth_delegate

    rkwargs['cert'] = auth.cert
    rkwargs['verify'] = verify

    # FIXUP for WFS in particular, remove xml style namespace
    # @TODO does this belong here?
    method = method.split("}")[-1]

    if method.lower() == 'post':
        try:
            etree.fromstring(data)
            headers['Content-Type'] = 'text/xml; charset=utf-8'
        except (ParseError, UnicodeEncodeError):
            pass

        rkwargs['data'] = data

    elif method.lower() == 'get':
        rkwargs['params'] = data

    else:
        raise ValueError("Unknown method ('%s'), expected 'get' or 'post'" % method)

    if cookies is not None:
        rkwargs['cookies'] = cookies

    req = requests.request(method.upper(), url_base, headers=headers, **rkwargs)

    if req.status_code == 400:
        raise ServiceException(req.text)

    if req.status_code in [401, 403, 404, 500, 502, 503, 504]:    # add more if needed
        req.raise_for_status()

    # check for service exceptions without the http header set
    if 'Content-Type' in req.headers and \
            req.headers['Content-Type'] in ['text/xml', 'application/xml', 'application/vnd.ogc.se_xml']:
        # just in case 400 headers were not set, going to have to read the xml to see if it's an exception report.
        se_tree = etree.fromstring(req.content)

        # to handle the variety of namespaces and terms across services
        # and versions, especially for "legacy" responses like WMS 1.3.0
        possible_errors = [
            '{http://www.opengis.net/ows}Exception',
            '{http://www.opengis.net/ows/1.1}Exception',
            '{http://www.opengis.net/ogc}ServiceException',
            'ServiceException'
        ]

        for possible_error in possible_errors:
            serviceException = se_tree.find(possible_error)
            if serviceException is not None:
                # and we need to deal with some message nesting
                raise ServiceException('\n'.join([t.strip() for t in serviceException.itertext() if t.strip()]))

    return ResponseWrapper(req)

package mqadmin

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	requestCodeUpdateAndCreateTopic                        = 17
	requestCodeUpdateBrokerConfig                          = 25
	requestCodeGetBrokerConfig                             = 26
	requestCodeGetBrokerRuntimeInfo                        = 28
	requestCodeQueryMessage                                = 12
	requestCodeViewMessageByID                             = 33
	requestCodePutKVConfig                                 = 100
	requestCodeDeleteKVConfig                              = 102
	requestCodeGetBrokerClusterInfo                        = 106
	requestCodeGetTopicStatsInfo                           = 202
	requestCodeUpdateAndCreateSubscriptionGroup            = 200
	requestCodeGetRouteInfoByTopic                         = 105
	requestCodeGetAllSubscriptionGroup                     = 201
	requestCodeGetConsumeStats                             = 208
	requestCodeGetConsumerConnectionList                   = 203
	requestCodeGetProducerConnectionList                   = 204
	requestCodeDeleteSubscriptionGroup                     = 207
	requestCodeGetAllTopicListFromNameSrv                  = 206
	requestCodeDeleteTopicInBroker                         = 215
	requestCodeDeleteTopicInNameSrv                        = 216
	requestCodeGetConsumerStatusFromClient                 = 221
	requestCodeInvokeBrokerToGetConsumerStatus             = 223
	requestCodeUpdateAndCreateSubscriptionGroupList        = 225
	requestCodeWipeWritePermOfBroker                       = 205
	requestCodeAddWritePermOfBroker                        = 327
	requestCodeInvokeBrokerToResetOffset                   = 222
	requestCodeCloneGroupOffset                            = 314
	requestCodeGetBrokerConsumeStats                       = 317
	requestCodeCleanUnusedTopic                            = 316
	requestCodeCleanExpiredConsumeQueue                    = 306
	requestCodeDeleteExpiredCommitLog                      = 329
	requestCodeGetAllProducerInfo                          = 328
	requestCodeQueryConsumeQueue                           = 321
	requestCodeCheckRocksdbCQWriteProgress                 = 354
	requestCodeExportRocksDBConfigToJSON                   = 355
	requestCodeUpdateNamesrvConfig                         = 318
	requestCodeGetNamesrvConfig                            = 319
	requestCodeAddBroker                                   = 902
	requestCodeRemoveBroker                                = 903
	requestCodeGetBrokerHAStatus                           = 907
	requestCodeResetMasterFlushOffset                      = 908
	requestCodeControllerElectMaster                       = 1002
	requestCodeControllerGetMetadataInfo                   = 1005
	requestCodeControllerGetSyncStateData                  = 1006
	requestCodeGetBrokerEpochCache                         = 1007
	requestCodeUpdateControllerConfig                      = 1009
	requestCodeGetControllerConfig                         = 1010
	requestCodeCleanBrokerData                             = 1011
	requestCodeUpdateColdDataFlowCtrConfig                 = 2001
	requestCodeRemoveColdDataFlowCtrConfig                 = 2002
	requestCodeGetColdDataFlowCtrInfo                      = 2003
	requestCodeSetCommitLogReadMode                        = 2004
	requestCodeAuthCreateUser                              = 3001
	requestCodeAuthUpdateUser                              = 3002
	requestCodeAuthDeleteUser                              = 3003
	requestCodeAuthGetUser                                 = 3004
	requestCodeAuthListUser                                = 3005
	requestCodeAuthCreateAcl                               = 3006
	requestCodeAuthUpdateAcl                               = 3007
	requestCodeAuthDeleteAcl                               = 3008
	requestCodeAuthGetAcl                                  = 3009
	requestCodeAuthListAcl                                 = 3010
	requestCodeSwitchTimerEngine                           = 5001
	requestCodeSetMessageRequestMode                       = 401
	requestCodeUpdateAndCreateStaticTopic                  = 513
	requestCodeGetBrokerMemberGroup                        = 901
	requestCodeGetBrokerLiteInfo                           = 200074
	requestCodeGetParentTopicInfo                          = 200075
	requestCodeGetLiteTopicInfo                            = 200076
	requestCodeGetLiteClientInfo                           = 200077
	requestCodeGetLiteGroupInfo                            = 200078
	requestCodeTriggerLiteDispatch                         = 200079
	responseCodeSuccess                                    = 0
	permWrite                                              = 2
	languageGo                                             = "GO"
	serializeJSON                                   byte   = 0
	remotingVersion                                 int    = 317
	defaultTimeout                                  int    = 5000
	defaultReadQueueNums                            int    = 8
	defaultWriteQueueNums                           int    = 8
	defaultPerm                                     int    = 6
	defaultTopicFilterType                          string = "SINGLE_TAG"
	defaultTopicSysFlag                             int    = 0
)

var opaqueCounter int32

type remotingCommand struct {
	Code      int               `json:"code"`
	Language  string            `json:"language"`
	Version   int               `json:"version"`
	Opaque    int32             `json:"opaque"`
	Flag      int               `json:"flag"`
	Remark    string            `json:"remark,omitempty"`
	ExtFields map[string]string `json:"extFields,omitempty"`
	Body      []byte            `json:"-"`
}

type remotingError struct {
	Code   int
	Remark string
}

func (e *remotingError) Error() string {
	return fmt.Sprintf("mqadmin: request failed code=%d remark=%s", e.Code, e.Remark)
}

type routeData struct {
	QueueDatas  []queueData  `json:"queueDatas"`
	BrokerDatas []brokerData `json:"brokerDatas"`
}

type queueData struct {
	BrokerName     string `json:"brokerName"`
	WriteQueueNums int    `json:"writeQueueNums"`
	Perm           int    `json:"perm"`
}

type brokerData struct {
	Cluster     string            `json:"cluster"`
	BrokerName  string            `json:"brokerName"`
	BrokerAddrs map[string]string `json:"brokerAddrs"`
}

func newCommand(code int, ext map[string]string) *remotingCommand {
	return &remotingCommand{
		Code:      code,
		Language:  languageGo,
		Version:   remotingVersion,
		Opaque:    atomic.AddInt32(&opaqueCounter, 1),
		Flag:      0,
		ExtFields: ext,
	}
}

func encodeFrame(cmd *remotingCommand) ([]byte, error) {
	header, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	frameLen := 4 + len(header)
	if len(cmd.Body) > 0 {
		frameLen += len(cmd.Body)
	}

	buf := make([]byte, 4+frameLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(frameLen))
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(header))|(uint32(serializeJSON)<<24))
	copy(buf[8:8+len(header)], header)
	if len(cmd.Body) > 0 {
		copy(buf[8+len(header):], cmd.Body)
	}
	return buf, nil
}

func decodeFrame(frame []byte) (*remotingCommand, error) {
	if len(frame) < 4 {
		return nil, fmt.Errorf("mqadmin: invalid remoting frame")
	}
	oriHeaderLen := binary.BigEndian.Uint32(frame[0:4])
	headerLen := int(oriHeaderLen & 0x00FFFFFF)
	if 4+headerLen > len(frame) {
		return nil, fmt.Errorf("mqadmin: invalid remoting header length")
	}

	cmd := &remotingCommand{}
	if err := json.Unmarshal(frame[4:4+headerLen], cmd); err != nil {
		return nil, err
	}
	if 4+headerLen < len(frame) {
		cmd.Body = frame[4+headerLen:]
	}
	return cmd, nil
}

func dial(ctx context.Context, addr string, useTLS bool, timeout time.Duration, tlsConfig *tls.Config) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	if useTLS {
		cfg := &tls.Config{}
		if tlsConfig != nil {
			cfg = tlsConfig.Clone()
		}
		return tls.DialWithDialer(dialer, "tcp", addr, cfg)
	}
	return dialer.DialContext(ctx, "tcp", addr)
}

func invokeSync(ctx context.Context, addr string, useTLS bool, timeout time.Duration, req *remotingCommand, tlsConfig *tls.Config) (*remotingCommand, error) {
	conn, err := dial(ctx, addr, useTLS, timeout, tlsConfig)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}

	packet, err := encodeFrame(req)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(packet); err != nil {
		return nil, err
	}

	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, err
	}
	frameSize := int(binary.BigEndian.Uint32(lenBuf))
	if frameSize <= 0 {
		return nil, fmt.Errorf("mqadmin: invalid response frame size")
	}
	body := make([]byte, frameSize)
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, err
	}

	resp, err := decodeFrame(body)
	if err != nil {
		return nil, err
	}
	if resp.Code != responseCodeSuccess {
		return nil, &remotingError{Code: resp.Code, Remark: resp.Remark}
	}
	return resp, nil
}

func invokeSyncWithRetry(ctx context.Context, addr string, useTLS bool, timeout time.Duration, req *remotingCommand, tlsConfig *tls.Config, retry int, backoff time.Duration) (*remotingCommand, error) {
	var lastErr error
	attempts := retry + 1
	if attempts < 1 {
		attempts = 1
	}
	for i := 0; i < attempts; i++ {
		resp, err := invokeSync(ctx, addr, useTLS, timeout, req, tlsConfig)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		var remErr *remotingError
		if errors.As(err, &remErr) {
			return nil, err
		}
		if i < attempts-1 && backoff > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return nil, lastErr
}

func toMapString(values map[string]any) map[string]string {
	out := make(map[string]string, len(values))
	for k, v := range values {
		switch t := v.(type) {
		case string:
			out[k] = t
		case bool:
			out[k] = strconv.FormatBool(t)
		case int:
			out[k] = strconv.Itoa(t)
		case int32:
			out[k] = strconv.FormatInt(int64(t), 10)
		case int64:
			out[k] = strconv.FormatInt(t, 10)
		default:
			out[k] = fmt.Sprint(t)
		}
	}
	return out
}

func propertiesToBytes(props map[string]string) []byte {
	if len(props) == 0 {
		return nil
	}
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b bytes.Buffer
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(props[k])
		b.WriteByte('\n')
	}
	return b.Bytes()
}

func bytesToProperties(data []byte) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		idx := strings.Index(line, "=")
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		if key != "" {
			out[key] = val
		}
	}
	return out
}

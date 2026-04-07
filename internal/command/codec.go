// 编码|解码
package command

import (
	commandpb "etcd-KV/internal/pb/command"

	"google.golang.org/protobuf/proto"
)

func Encode(cmd *Command) ([]byte, error) {
	return proto.Marshal(toPBCommand(cmd))
}

func Decode(data []byte, dst *Command) (error) {
	var pbCmd commandpb.Command

	if err := proto.Unmarshal(data, &pbCmd); err != nil {
		return err
	}

	decoded := fromPBCommand(&pbCmd)
	if decoded == nil {
		*dst = Command{}
		return nil
	}

	*dst = *decoded
	return nil
}
package docker

import (
	"github.com/docker/docker/graph"
	"github.com/docker/docker/graph/tags"
	"github.com/docker/docker/pkg/parsers"
	"github.com/docker/docker/registry"
	"github.com/golang/glog"
)

func (cli Docker) SendCmdPull(image string, imagePullConfig *graph.ImagePullConfig) ([]byte, int, error) {
	// We need to create a container via an image object.  If the image
	// is not stored locally, so we need to pull the image from the Docker HUB.

	// Get a Repository name and tag name from the argument, but be careful
	// with the Repository name with a port number.  For example:
	//      localdomain:5000/samba/hipache:latest
	repository, tag := parsers.ParseRepositoryTag(image)
	if err := registry.ValidateRepositoryName(repository); err != nil {
		return nil, -1, err
	}
	if len(tag) > 0 {
		if err := tags.ValidateTagName(tag); err != nil {
			return nil, -1, err
		}
	} else {
		tag = tags.DefaultTag
	}

	glog.V(3).Infof("The Repository is %s, and the tag is %s", repository, tag)
	glog.V(3).Info("pull the image from the repository!")
	err := cli.daemon.Repositories().Pull(repository, tag, imagePullConfig)
	if err != nil {
		return nil, -1, err
	}
	return nil, 200, nil
}

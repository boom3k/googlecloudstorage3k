package googlecloudstorage3k

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"google.golang.org/api/option"
	"io/ioutil"
	"log"
	"mime"
	"path"
	"strings"
	"time"
)

type API struct {
	Client     *storage.Client
	AdminEmail string
	Domain     string
}

func Build(adminEmail string, serviceAccountKey []byte, ctx context.Context) *API {
	newClient, err := storage.NewClient(ctx, option.WithCredentialsJSON(serviceAccountKey))
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}
	newCloudStorage3k := &API{Client: newClient, AdminEmail: adminEmail, Domain: strings.Split(adminEmail, "@")[1]}
	log.Printf("CloudStorage3k Ready @ --> [%v]\n", &newCloudStorage3k)
	return newCloudStorage3k
}

func (receiver *API) UploadData(bucketName, fileName string, data []byte, timeout int) (*BucketObject, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	bucketWriter := receiver.Client.Bucket(bucketName).Object(fileName).NewWriter(ctx)
	bucketWriter.ContentType = mime.TypeByExtension(path.Ext(fileName))
	defer bucketWriter.Close()
	sizeUploaded, err := bucketWriter.Write(data)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	log.Printf("Uploaded %s[%s] to --> {%s}\n", fileName, ByteCountSI(int64(sizeUploaded)), bucketName)
	return &BucketObject{Filename: fileName, BucketName: bucketName, Data: data}, nil
}

func (receiver *API) UploadObject(object *BucketObject, timeout int) (*BucketObject, error) {
	return receiver.UploadData(object.BucketName, object.Filename, object.Data, timeout)
}

func (receiver *API) DownloadObject(bucketName, objectName string, timeout int) (*BucketObject, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()

	bucketReader, err := receiver.Client.Bucket(bucketName).Object(objectName).NewReader(ctx)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	defer bucketReader.Close()

	data, err := ioutil.ReadAll(bucketReader)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &BucketObject{
			BucketName: bucketName,
			Filename:   objectName,
			Data:       data},
		nil
}

type BucketObject struct {
	BucketName string
	Filename   string
	Data       []byte
}

func NewBucketObject(bucketName, fileName string, data []byte) *BucketObject {
	return &BucketObject{
		BucketName: bucketName,
		Filename:   fileName,
		Data:       data,
	}
}

func (receiver *BucketObject) Upload(gcs3k *API, timeout int) {
	receiver, err := gcs3k.UploadData(receiver.BucketName, receiver.Filename, receiver.Data, timeout)
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}
}

func ByteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}
